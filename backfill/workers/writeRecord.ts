import { MemoryCache } from "@atproto/identity";
import { lexToJson } from "@atproto/lexicon";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import { CID } from "multiformats/cid";
import { IdResolver, IndexingService } from "../indexingService";
import type { CommitMessage } from "./repo";
import type { ToInsertCommit } from "./writeCollection";
import type { FromWorkerMessage } from "../main";

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

	setTimeout(function forceGC() {
		Bun.gc(true);
		setTimeout(forceGC, 30_000);
	}, 30_000);

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
			await Bun.write(
				`./failed-records.jsonl`,
				records.map((r) =>
					JSON.stringify({
						uri: r.uri.toString(),
						cid: r.cid.toString(),
						timestamp: r.timestamp,
						obj: lexToJson(r.obj),
					})
				).join("\n") + "\n",
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
