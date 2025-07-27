import { MemoryCache } from "@atproto/identity";
import { lexToJson } from "@atproto/lexicon";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import { CID } from "multiformats/cid";
import fs from "node:fs/promises";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { FromWorkerMessage } from "../main.js";
import type { CommitData } from "./repo.js";
import type { ToInsertCommit } from "./writeCollection.js";

export async function writeRecordWorker() {
	console.info(`Starting write record worker`);

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 20,
		poolIdleTimeoutMs: 30_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		fallbackPlc: process.env.FALLBACK_PLC_URL,
		// 1m stale, 2m max; very short because we should only really see any given identity once or twice
		didCache: new MemoryCache(1 * 60 * 1000, 2 * 60 * 1000),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	let toIndexRecords: ToInsertCommit[] = [];

	let recordQueueTimer = setTimeout(processRecordQueue, 500);

	let isShuttingDown = false;

	const queue = new Queue<{ commits: CommitData[] }>("records-write", {
		removeOnSuccess: true,
		removeOnFailure: true,
		isWorker: true,
	});

	queue.on("error", (err) => {
		console.error(`Queue error for records write:`, err);
	});

	queue.on("failed", (_, err) => {
		console.error(`Job failed for records write:`, err);
	});

	queue.process(10, async (job) => {
		if (isShuttingDown) return;
		const { commits } = job.data;

		for (const commit of commits) {
			const { did, path, cid: _cid, timestamp, obj: obj } = commit;
			if (!did || !path || !_cid || !timestamp || !obj) {
				throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
			}

			const uri = new AtUri(`at://${did}/${path}`);
			const cid = CID.parse(_cid);

			// jsonToLex(obj) unnecessary because the record just gets stringified
			toIndexRecords.push({ uri, cid, timestamp, obj });
		}

		if (toIndexRecords.length > 100_000) {
			clearTimeout(recordQueueTimer);
			await processRecordQueue();
		}
	});

	process.on("message", async (msg: { type: "shutdown" }) => {
		if (msg.type === "shutdown") {
			await handleShutdown();
			return;
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
			console.error(`Error processing records queue`, err);
			await fs.writeFile(
				`./failed-records.jsonl`,
				records.map((r) =>
					JSON.stringify({
						uri: r.uri.toString(),
						cid: r.cid.toString(),
						timestamp: r.timestamp,
						obj: lexToJson(r.obj),
					})
				).join("\n") + "\n",
				{ flag: "a" },
			);
			console.timeEnd(time);
		}
	}

	async function handleShutdown() {
		console.log("Write record worker received shutdown signal");
		isShuttingDown = true;
		await processRecordQueue();
		process.send?.({ type: "shutdownComplete" } satisfies FromWorkerMessage);
		process.exit(0);
	}
}
