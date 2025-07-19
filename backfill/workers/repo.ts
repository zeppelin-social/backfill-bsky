import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { MemoryCache } from "@atproto/identity";
import { createClient } from "@redis/client";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import fs from "node:fs/promises";
import path from "node:path";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../indexingService";
import { is, lexicons } from "../util/lexicons";

export type CommitData = {
	did: string;
	path: string;
	cid: string;
	timestamp: string;
	obj: unknown;
};

export type CommitMessage = { type: "commit"; collection: string; commits: CommitData[] };

export async function repoWorker() {
	for (const envVar of ["REPOS_DIR"]) {
		if (!process.env[envVar]) {
			throw new Error(`Repo worker missing env var ${envVar}`);
		}
	}

	const redis = createClient();
	await redis.connect();

	const queue = new Queue<{ did: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
	});

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 20,
		poolIdleTimeoutMs: 60_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		fallbackPlc: process.env.FALLBACK_PLC_URL,
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	let isShuttingDown = false;

	let commitData: Record<string, CommitData[]> = {};

	const indexActorQueue = new PQueue({ concurrency: 2 });
	const toIndexDids = new Set<string>();

	queue.process(25, async (job) => {
		if (!process?.send) throw new Error("Not a worker process");

		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		let repo: Uint8Array | null;
		try {
			repo = Bun.mmap(path.join(process.env.REPOS_DIR!, did));
			if (!repo?.byteLength) throw "Got empty repo";
		} catch (err) {
			if (`${err}`.includes("ENOENT")) return;
			console.warn("Error while getting repo bytes for " + did, err);
			return;
		}

		try {
			const now = Date.now();
			for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
				const path = `${collection}/${rkey}`;

				if (!is(collection, record)) { // This allows us to set { validate: false } in the collection worker
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
					path,
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
		} finally {
			repo.fill(0, 0, -1);
			repo = null;
			await fs.unlink(path.join(process.env.REPOS_DIR!, did));
		}
	});

	queue.on("error", (err) => {
		console.error("Queue error:", err);
		handleShutdown();
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
		handleShutdown();
	});

	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	async function processActorQueue() {
		if (!isShuttingDown) {
			setTimeout(processActorQueue, 10_000);
		}

		if (toIndexDids.size > 0) {
			const dids = [...toIndexDids];
			toIndexDids.clear();
			void indexActorQueue.add(async () => {
				try {
					const time = `Indexing actors: ${dids.length}`;
					console.time(time);
					await indexingSvc.indexActorsBulk(dids);
					console.timeEnd(time);
				} catch (e) {
					console.error(`Error while indexing actors: ${e}`);
					await Bun.write(`./failed-actors.jsonl`, dids.join(",") + "\n");
				}
			});
		}
	}

	setTimeout(function sendCommits() {
		const entries = Object.entries(commitData);
		for (const [collection, commits] of entries) {
			process.send!({ type: "commit", collection, commits } satisfies CommitMessage);
			commitData[collection] = [];
		}
		commitData = {};
		setTimeout(sendCommits, 200);
	}, 200);

	// Stagger the logging a bit the first time
	setTimeout(processActorQueue, 10_000 + (Math.random() * 15_000));

	setTimeout(function forceGC() {
		Bun.gc(true);
		setTimeout(forceGC, 30_000);
	}, 30_000);

	async function handleShutdown() {
		isShuttingDown = true;
		await processActorQueue();
		process.exit(1);
	}
}
