import { IdResolver, MemoryCache } from "@atproto/identity";
import { BlobRef } from "@atproto/lexicon";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { IndexingService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing/index";
import { CID } from "multiformats/cid";
import type { CommitMessage } from "./repo.js";

export type ToInsertCommit = { uri: AtUri; cid: CID; timestamp: string; obj: unknown };

// 3 write workers, each handles 5 record types
// picked largely based on vibes to kind of evenly distribute load
export const writeWorkerAllocations = [[
	"app.bsky.feed.post",
	"chat.bsky.actor.declaration",
	"app.bsky.feed.postgate",
	"app.bsky.labeler.service",
	"app.bsky.feed.generator",
], [
	"app.bsky.feed.like",
	"app.bsky.actor.profile",
	"app.bsky.graph.list",
	"app.bsky.graph.block",
	"app.bsky.graph.starterpack",
], [
	"app.bsky.feed.threadgate",
	"app.bsky.feed.repost",
	"app.bsky.graph.follow",
	"app.bsky.graph.listitem",
	"app.bsky.graph.listblock",
]];

export async function writeCollectionWorker() {
	const workerIndex = parseInt(process.env.WORKER_INDEX || "-1");
	const collections = writeWorkerAllocations[workerIndex];

	if (!collections) throw new Error(`Invalid worker index ${workerIndex}`);
	console.info(`Starting write worker ${workerIndex} for ${collections.join(", ")}`);

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 50,
		poolIdleTimeoutMs: 60_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	const queues: Record<string, ToInsertCommit[]> = {};

	for (const collection of collections) {
		if (!indexingSvc.findIndexerForCollection(collection)) {
			throw new Error(`No indexer for collection ${collection}`);
		}
		queues[collection] = [];
	}

	let queueTimer = setTimeout(processQueue, 1000);

	let isShuttingDown = false;

	process.on("message", async (msg: CommitMessage | { type: "shutdown" }) => {
		if (msg.type === "shutdown") {
			console.log(`Write collection worker ${workerIndex} received shutdown signal`);
			isShuttingDown = true;
			// Process remaining queue then exit
			await processQueue();
			process.send?.({ type: "shutdownComplete" });
			process.exit(0);
		}

		if (isShuttingDown) return; // Don't accept new messages during shutdown

		if (msg.type !== "commit") {
			throw new Error(`Invalid message type ${msg}`);
		}

		if (!queues[msg.collection]) {
			console.warn(`Received commit for unknown collection ${msg.collection}`);
			return;
		}

		for (const commit of msg.commits) {
			const { uri, cid, timestamp, obj } = commit;
			if (!uri || !cid || !timestamp || !obj) {
				throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
			}

			// The appview IndexingService does lex validation on the record, which only accepts blob refs in the
			// form of a BlobRef instance, so we need to do this expensive iteration over every single record
			convertBlobRefs(obj);

			queues[msg.collection].push({
				uri: new AtUri(uri),
				cid: CID.parse(cid),
				timestamp,
				obj,
			});
		}

		if (queues[msg.collection].length > 500_000) {
			clearTimeout(queueTimer);
			queueTimer = setImmediate(processQueue);
		}
	});

	process.on("uncaughtException", (err) => {
		console.error(`Uncaught exception in worker ${workerIndex}`, err);
	});

	async function processQueue() {
		if (!isShuttingDown) {
			queueTimer = setTimeout(processQueue, 1000);
		}

		let recordCount = 0;
		const records = new Map<string, ToInsertCommit[]>();

		for (const collection in queues) {
			if (queues[collection].length > 0) {
				recordCount += queues[collection].length;
				records.set(collection, queues[collection]);
				queues[collection] = [];
			}
		}

		const time = `Writing ${recordCount} records by collection for ${collections.join(", ")}`;

		try {
			if (recordCount > 0) {
				console.time(time);
				await indexingSvc.bulkIndexToCollectionSpecificTables(records);
				console.timeEnd(time);
			}
		} catch (err) {
			console.error(`Error processing queue for ${collections.join(", ")}`, err);
			console.timeEnd(time);
		}
	}
}

export function convertBlobRefs(obj: unknown): unknown {
	if (!obj) return obj;
	if (Array.isArray(obj)) {
		for (let i = 0; i < obj.length; i++) {
			obj[i] = convertBlobRefs(obj[i]);
		}
	} else if (typeof obj === "object") {
		const record = obj as Record<string, any>;

		// weird-ish formulation but faster than for-in or Object.entries
		const keys = Object.keys(record);
		let i = keys.length;
		while (i--) {
			const key = keys[i];
			const value = record[key];
			if (typeof value === "object" && value !== null) {
				if (value.$type === "blob") {
					try {
						const cidLink = CID.parse(value.ref.$link);
						record[key] = new BlobRef(cidLink, value.mimeType, value.size);
					} catch {
						console.warn(
							`Failed to parse CID ${value.ref.$link}\nRecord: ${
								JSON.stringify(record)
							}`,
						);
						return record;
					}
				} else {
					convertBlobRefs(value);
				}
			}
		}
	}

	return obj;
}
