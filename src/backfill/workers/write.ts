import { IdResolver, MemoryCache } from "@atproto/identity";
import { BlobRef } from "@atproto/lexicon";
import { AtUri } from "@atproto/syntax";
import * as bsky from "@futuristick/atproto-bsky";
import { CID } from "multiformats/cid";
import type { CommitMessage } from "./repo.js";

type ToInsertCommit = { uri: AtUri; cid: CID; timestamp: string; obj: unknown };

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

export async function writeWorker() {
	const workerIndex = parseInt(process.env.WORKER_INDEX || "-1");
	const collections = writeWorkerAllocations[workerIndex];

	if (!collections) throw new Error(`Invalid worker index ${workerIndex}`);
	console.info(`Starting write worker ${workerIndex} for ${collections.join(", ")}`);

	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 5,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const { indexingSvc } = new bsky.RepoSubscription({
		service: process.env.BSKY_REPO_PROVIDER,
		db,
		idResolver,
	});

	const queues: Record<string, Set<ToInsertCommit>> = {};
	{}

	for (const collection of collections) {
		if (!indexingSvc.findIndexerForCollection(collection)) {
			throw new Error(`No indexer for collection ${collection}`);
		}
		queues[collection] = new Set();
	}

	process.on("message", async (msg: CommitMessage) => {
		if (msg.type !== "commit") throw new Error(`Invalid message type ${msg.type}`);

		const { uri, cid, timestamp, obj } = msg.data;
		if (!uri || !cid || !timestamp || !obj) {
			throw new Error(`Invalid commit data ${JSON.stringify(msg.data)}`);
		}
		if (!queues[msg.collection]) return;

		// The appview IndexingService does lex validation on the record, which only accepts blob refs in the
		// form of a BlobRef instance, so we need to do this expensive iteration over every single record
		convertBlobRefs(obj);

		queues[msg.collection].add({ uri: new AtUri(uri), cid: CID.parse(cid), timestamp, obj });
	});

	setTimeout(async function processQueue() {
		try {
			const records = new Map<string, ToInsertCommit[]>();
			for (const collection of collections) {
				if (queues[collection].size > 0) {
					records.set(collection, Array.from(queues[collection]));
					queues[collection].clear();
				}
			}

			if (records.size > 0) {
				console.time(`Writing records for ${collections.join(", ")}`);
				await indexingSvc.indexRecordsBulkAcrossCollections(records);
				console.timeEnd(`Writing records for ${collections.join(", ")}`);
			}
		} catch (err) {
			console.error(`Error processing queue for ${collections.join(", ")}`, err);
			console.timeEnd(`Writing records for ${collections.join(", ")}`);
		} finally {
			setTimeout(processQueue, 1000);
		}
	}, 1000);
}

function convertBlobRefs(obj: unknown): unknown {
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
