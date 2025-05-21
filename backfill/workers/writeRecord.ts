import { MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { CID } from "multiformats/cid";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { CommitMessage } from "./repo.js";
import { jsonToLex, type ToInsertCommit } from "./writeCollection.js";

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

	const seenDids = new Set<string>();
	const toIndexDids = new Set<string>();
	const indexActorQueue = new PQueue({ concurrency: 5 });

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
			const { uri: _uri, cid: _cid, timestamp, obj: _obj } = commit;
			if (!_uri || !_cid || !timestamp || !_obj) {
				throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
			}

			const uri = new AtUri(_uri);
			const cid = CID.parse(_cid);
			const obj = jsonToLex(_obj as Record<string, unknown>);

			toIndexRecords.push({ uri, cid, timestamp, obj });

			const did = uri.host;
			if (!seenDids.has(did)) {
				toIndexDids.add(did);
				seenDids.add(did);
			}
		}

		if (toIndexRecords.length > 200_000) {
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

		const records = [...toIndexRecords];
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
				const time = `Indexing actors: ${dids.length}`;
				console.time(time);
				await indexingSvc.indexActorsBulk(dids);
				console.timeEnd(time);
			}).catch((e) => {
				console.error(`Error while indexing actors: ${e}`);
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
