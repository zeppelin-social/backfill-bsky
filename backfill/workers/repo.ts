import { RepoReader } from "@atcute/car/v4";
import { parse as parseTID } from "@atcute/tid";
import { MemoryCache } from "@atproto/identity";
import { createClient } from "@redis/client";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import fs from "node:fs/promises";
import path from "node:path";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { FromWorkerMessage } from "../main.js";
import { is } from "../util/lexicons.js";

export type CommitData = {
	did: string;
	path: string;
	cid: string;
	timestamp: string;
	obj: unknown;
};

export type CommitMessage = {
	type: "commit";
	did?: string;
	collection: string;
	commits: CommitData[];
};

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
		poolIdleTimeoutMs: 60_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		fallbackPlc: process.env.FALLBACK_PLC_URL,
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	let isShuttingDown = false;

	const indexActorQueue = new PQueue({ concurrency: 2 });
	const toIndexDids = new Set<string>();
	let actorsIndexed = 0;

	let i = 0;
	queue.process(10, async (job) => {
		if (!process?.send) throw new Error("Not a worker process");

		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		let repo: Uint8Array | null;
		try {
			repo = await fs.readFile(path.join(process.env.REPOS_DIR!, did));
			if (!repo?.byteLength) throw "Got empty repo";
		} catch (err) {
			if (!`${err}`.includes("ENOENT")) {
				console.warn("Error while getting repo bytes for " + did, err);
			}
			return;
		}

		const commitData: Record<string, CommitData[]> = {};

		try {
			const now = Date.now();
			const reader = RepoReader.fromUint8Array(repo);
			for await (const { record, rkey, collection, cid } of reader) {
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

			const entries = Object.entries(commitData);
			for (const [collection, commits] of entries) {
				process.send!(
					{ type: "commit", did, collection, commits } satisfies FromWorkerMessage,
				);
			}

			i++;
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
			if (`${err}`.includes("invalid simple value")) {
				console.warn(`Marking broken bridgy repo ${did} as seen`);
				await redis.sAdd("backfill:seen", did);
			}
		} finally {
			repo = null;
			await fs.unlink(path.join(process.env.REPOS_DIR!, did));
		}
	});

	queue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
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

	// setTimeout(function sendCommits() {
	// const entries = Object.entries(commitData);
	// commitData = {};
	// for (const [collection, commits] of entries) {
	// 	process.send!({ type: "commit", collection, commits } satisfies FromWorkerMessage);
	// }
	// setTimeout(sendCommits, 200);
	// }, 200);

	setTimeout(processActorQueue, 10_000);

	setInterval(() => {
		console.log(`parsed repos: ${(i / 5).toFixed(1)}/s`);
		i = 0;
	}, 5000);

	setTimeout(function logIndexedActors() {
		console.log(`Indexed actors: ${actorsIndexed}`);
		actorsIndexed = 0;
		setTimeout(logIndexedActors, 60_000);
	}, Math.random() * 60_000); // Spread out the logging a bit so there isn't a barrage of these

	async function handleShutdown() {
		isShuttingDown = true;
		await processActorQueue();
		process.send?.({ type: "shutdownComplete" } satisfies FromWorkerMessage);
		process.exit(1);
	}
}
