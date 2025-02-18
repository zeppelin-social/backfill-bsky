import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { IndexingService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";
import { CID } from "multiformats/cid";
import type { CommitMessage } from "./repo.js";
import { convertBlobRefs, type ToInsertCommit } from "./writeCollection";

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
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	let queue: ToInsertCommit[] = [];

	let queueTimer = setTimeout(processQueue, 500);

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
			const { uri, cid, timestamp, obj } = commit;
			if (!uri || !cid || !timestamp || !obj) {
				throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
			}

			// The appview IndexingService does lex validation on the record, which only accepts blob refs in the
			// form of a BlobRef instance, so we need to do this expensive iteration over every single record
			convertBlobRefs(obj);

			queue.push({ uri: new AtUri(uri), cid: CID.parse(cid), timestamp, obj });
		}

		if (queue.length > 200_000) {
			clearTimeout(queueTimer);
			queueTimer = setImmediate(processQueue);
		}
	});

	process.on("uncaughtException", (err) => {
		console.error(`Uncaught exception in write record worker`, err);
	});

	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	async function processQueue() {
		if (!isShuttingDown) {
			queueTimer = setTimeout(processQueue, 500);
		}

		const time = `Writing records: ${queue.length}`;

		const records = [...queue];
		queue = [];

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

	async function handleShutdown() {
		console.log("Write record worker received shutdown signal");
		isShuttingDown = true;
		await processQueue();
		process.send?.({ type: "shutdownComplete" });
		process.exit(0);
	}
}
