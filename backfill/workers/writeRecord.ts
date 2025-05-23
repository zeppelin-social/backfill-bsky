import { MemoryCache } from "@atproto/identity";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { LRUCache } from "lru-cache";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { CommitMessage } from "./repo.js";
import type { ToInsertCommit } from "./writeCollection.js";

export async function writeRecordWorker() {
	console.info(`Starting write record worker`);

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

	let toIndexRecords: ToInsertCommit[] = [];

	let recordQueueTimer = setTimeout(processRecordQueue, 500);
	setTimeout(processActorQueue, 2000);

	setTimeout(function forceGC() {
		Bun.gc(true);
		setTimeout(forceGC, 30_000);
	}, 30_000);

	const seenDids = new LRUCache<string, boolean>({ max: 10_000 });
	const toIndexDids = new Set<string>();
	const indexActorQueue = new PQueue({ concurrency: 2 });

	let isShuttingDown = false;

	process.on("message", async (msg: CommitMessage | { type: "shutdown" }) => {
		if (msg.type === "shutdown") {
			await handleShutdown();
			return;
		}

		if (isShuttingDown) return; // Don't accept new messages during shutdown

		if (msg.type !== "commit") {
			throw new Error(`Invalid message type ${msg}`);
		}

		for (const commit of msg.commits) {
			const { did, path, cid, timestamp, obj: obj } = commit;
			if (!did || !path || !cid || !timestamp || !obj) {
				throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
			}

			// jsonToLex unnecessary because the record just gets stringified
			toIndexRecords.push({ did, path, cid, timestamp, obj });

			if (!seenDids.has(did)) {
				toIndexDids.add(did);
				seenDids.set(did, true);
			}
		}

		if (toIndexRecords.length > 100_000) {
			clearTimeout(recordQueueTimer);
			recordQueueTimer = setImmediate(processRecordQueue);
		}
	});

	process.on("uncaughtException", (err) => {
		console.error(`Uncaught exception in write record worker`, err);
	});

	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	async function processRecordQueue() {
		if (!isShuttingDown) {
			recordQueueTimer = setTimeout(processRecordQueue, 500);
		}

		const time = `Writing records: ${toIndexRecords.length}`;

		const records = toIndexRecords;
		toIndexRecords = [];

		try {
			if (records.length > 0) {
				console.time(time);
				await indexingSvc.bulkIndexToRecordTable(records);
				console.timeEnd(time);
			}
		} catch (err) {
			console.error(`Error processing queue`, err);
			console.timeEnd(time);
		}
	}

	async function processActorQueue() {
		if (!isShuttingDown) {
			setTimeout(processActorQueue, 2000);
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
				}
			});
		}
	}

	async function handleShutdown() {
		console.log("Write record worker received shutdown signal");
		isShuttingDown = true;
		await processRecordQueue();
		await processActorQueue();
		process.send?.({ type: "shutdownComplete" });
		process.exit(0);
	}
}
