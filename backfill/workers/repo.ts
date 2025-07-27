import { RepoReader } from "@atcute/car/v4";
import { parse as parseTID } from "@atcute/tid";
import { createClient } from "@redis/client";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import fs from "node:fs/promises";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { FromWorkerMessage } from "../main.js";
import { LRUDidCache } from "../util/cache.js";
import { is } from "../util/lexicons.js";
import { RetryError, XRPCManager } from "../util/xrpc.js";
import { writeWorkerAllocations } from "./writeCollection.js";

export type CommitData = {
	did: string;
	path: string;
	cid: string;
	timestamp: string;
	obj: unknown;
};

export type CommitMessage = { type: "commit"; collection: string; commits: CommitData[] };

export async function repoWorker() {
	const redis = createClient();
	await redis.connect();

	const queue = new Queue<{ did: string; pds: string }>("repo-processing", {
		sendEvents: false,
		storeJobs: false,
		removeOnSuccess: true,
		removeOnFailure: true,
		isWorker: true,
	});

	const writeQueues = {
		records: new Queue<{ commits: CommitData[] }>("records-write", {
			getEvents: false,
			storeJobs: false,
			removeOnSuccess: true,
			removeOnFailure: true,
			isWorker: false,
		}),
		opensearch: new Queue<{ collection: string; commits: CommitData[] }>("opensearch-write", {
			getEvents: false,
			storeJobs: false,
			removeOnSuccess: true,
			removeOnFailure: true,
			isWorker: false,
		}),
		collections: Object.fromEntries(
			writeWorkerAllocations.flat().map((
				collection,
			) => [
				collection,
				new Queue<{ commits: CommitData[] }>(`collection-write-${collection}`, {
					getEvents: false,
					storeJobs: false,
					removeOnSuccess: true,
					removeOnFailure: true,
					isWorker: false,
				}),
			]),
		),
	};

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolIdleTimeoutMs: 30_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		fallbackPlc: process.env.FALLBACK_PLC_URL,
		didCache: new LRUDidCache(10_000),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	const xrpc = new XRPCManager();

	let isShuttingDown = false;

	let commitData: Record<string, CommitData[]> = {};

	const processRepoPQueue = new PQueue({ concurrency: 50 });
	const indexActorPQueue = new PQueue({ concurrency: 2 });
	const toIndexDids = new Set<string>();

	let fetched = 0, parsed = 0;
	let actorsIndexed = 0;

	queue.process(30, (job) => processRepoPQueue.add(() => processRepo(job)));

	queue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});

	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	let sendTimer = setTimeout(sendCommits, 300);

	setTimeout(processActorQueue, 10_000);

	setTimeout(function logIndexedActors() {
		if (actorsIndexed > 0) {
			console.log(`Indexed actors: ${actorsIndexed}`);
		}
		actorsIndexed = 0;
		setTimeout(logIndexedActors, 60_000);
	}, Math.random() * 60_000); // Spread out the logging a bit so there isn't a barrage of these

	setInterval(() => {
		process.send?.({ type: "count", fetched, parsed } satisfies FromWorkerMessage);
		fetched = 0;
		parsed = 0;
	}, 1000);

	const fmt = (n: number) => (n / 1024 / 1024).toFixed(1) + " MB";

	setInterval(async () => {
		const mu = process.memoryUsage();
		const buf = fmt(mu.arrayBuffers);
		const heap = fmt(mu.heapUsed);
		const rss = fmt(mu.rss);

		global.gc?.();
		const mu2 = process.memoryUsage();
		const heap2 = fmt(mu2.heapUsed);

		const pendingCommits = Object.values(commitData).reduce((a, c) => a + c.length, 0);

		const { waiting, active } = await queue.checkHealth();

		const queued = processRepoPQueue.size;
		const running = processRepoPQueue.pending;

		console.log(
			`[mem ${process.pid}] buffers=${buf} rss=${rss} heap=${heap} (after gc=${heap2}) commits=${pendingCommits} queue waiting=${waiting} active=${active} queued=${queued} running=${running}`,
		);
	}, 10_000);

	async function processRepo(job: Queue.Job<{ did: string; pds: string }>, attempt = 0) {
		if (!process?.send) throw new Error("Not a worker process");

		const { did, pds } = job.data;

		if (!did || typeof did !== "string" || !pds || typeof pds !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		let bytes;
		try {
			bytes = await xrpc.query(
				pds,
				async (client) =>
					await client.get("com.atproto.sync.getRepo", {
						params: { did: did as `did:plc:${string}` },
						as: "stream",
					}),
				attempt,
			);
		} catch (err) {
			if (err instanceof RetryError) {
				void processRepoPQueue.add(async () => {
					await err.wait();
					return processRepo(job, err.attempt + 1);
				});
			} else if (
				["RepoDeactivated", "RepoTakendown", "RepoNotFound", "NotFound"].some((s) =>
					`${err}`.includes(s)
				)
			) {
				await redis.sAdd("backfill:seen", did);
			} else {
				console.error(`Error fetching repo for ${did} --- ${err}`);
			}
			return;
		}

		fetched++;

		try {
			const now = Date.now();
			const repo = RepoReader.fromStream(bytes);
			for await (const { record, rkey, collection, cid } of repo) {
				if (!(collection in writeQueues.collections) || !is(collection, record)) {
					// This allows us to set { validate: false } in the collection worker
					continue;
				}

				// This should be the date the AppView saw the record, but since we don't want the "archived post" label
				// to show for every post in social-app, we'll try our best to figure out when the record was actually created.
				// So first we try createdAt then parse the rkey; if either of those is in the future, we'll use now.
				let indexedAt: number =
					(!!record && typeof record === "object" && "createdAt" in record
						&& typeof record.createdAt === "string"
						&& new Date(record.createdAt).getTime()) || 0;
				if (!indexedAt || isNaN(indexedAt)) {
					try {
						indexedAt = parseTID(rkey).timestamp;
					} catch {
						indexedAt = now;
					}
				}
				if (indexedAt > now) indexedAt = now;

				const data = {
					did,
					path: `${collection}/${rkey}`,
					cid: cid.$link,
					timestamp: new Date(indexedAt).toISOString(),
					obj: record,
				};

				(commitData[collection] ??= []).push(data);
			}

			await redis.sAdd("backfill:seen", did);
			toIndexDids.add(did);
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
			if (`${err}`.includes("invalid simple value")) {
				console.warn(`Marking broken bridgy repo ${did} as seen`);
				await redis.sAdd("backfill:seen", did);
			}
		}

		parsed++;
	}

	async function sendCommits() {
		clearTimeout(sendTimer);
		sendTimer = setTimeout(sendCommits, 300);

		const entries = Object.entries(commitData);
		commitData = {};

		const promises: Promise<unknown>[] = [];

		if (entries.length) {
			promises.push(
				writeQueues.records.saveAll(
					entries.map((
						[_collection, commits],
					) => (writeQueues.records.createJob({ commits }))),
				).catch((err) => {
					console.error(`Error saving records commits:`, err);
				}),
			);
		}

		for (const [collection, commits] of entries) {
			if (!commits.length) continue;

			promises.push(
				writeQueues.collections[collection].saveAll(commits.map((commit) =>
					writeQueues.collections[collection].createJob({ commits: [commit] })
				)).catch((err) => {
					console.error(`Error saving commits for ${collection}:`, err);
				}),
			);

			if (
				(collection === "app.bsky.feed.post" || collection === "app.bsky.actor.profile")
				&& process.env.OPENSEARCH_URL
			) {
				promises.push(
					writeQueues.opensearch.saveAll(commits.map((commit) =>
						writeQueues.opensearch.createJob({ collection, commits: [commit] })
					)).catch((err) => {
						console.error(`Error saving opensearch commits for ${collection}:`, err);
					}),
				);
			}
		}

		if (promises.length) {
			console.time(`Saving ${entries.length} commits`);
			await Promise.allSettled(promises);
			console.timeEnd(`Saving ${entries.length} commits`);
		}
	}

	async function processActorQueue() {
		if (!isShuttingDown) {
			setTimeout(processActorQueue, 10_000);
		}

		if (toIndexDids.size > 0) {
			const dids = [...toIndexDids];
			toIndexDids.clear();
			void indexActorPQueue.add(async () => {
				try {
					await indexingSvc.indexActorsBulk(dids);
					actorsIndexed += dids.length;
				} catch (e) {
					console.error(`Error while indexing actors: ${e}`);
					await fs.writeFile(`./failed-actors.jsonl`, dids.join(",") + "\n", {
						flag: "a",
					});
				}
			});
		}
	}

	async function handleShutdown() {
		isShuttingDown = true;
		await processActorQueue();
		process.send?.({ type: "shutdownComplete" } satisfies FromWorkerMessage);
		process.exit(1);
	}
}
