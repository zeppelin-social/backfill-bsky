import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import * as bsky from "@futuristick/atproto-bsky";
import type RecordProcessor from "@futuristick/atproto-bsky/dist/data-plane/server/indexing/processor";
import { CID } from "multiformats/cid";
import type { CommitMessage } from "./repo.js";

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
		poolSize: 1,
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

	const indexers: Record<string, RecordProcessor<unknown, unknown>> = {};
	const queues: Record<string, Set<{ uri: AtUri; cid: CID; timestamp: string; obj: unknown }>> =
		{};

	for (const collection of collections) {
		const indexer = indexingSvc.findIndexerForCollection(collection);
		if (!indexer) throw new Error(`No indexer for collection ${collection}`);
		indexers[collection] = indexer;
		queues[collection] = new Set();
	}

	process.on("message", async (msg: CommitMessage) => {
		if (msg.type !== "commit") throw new Error(`Invalid message type ${msg.type}`);

		const { uri, cid, timestamp, obj } = msg.data;
		if (!uri || !cid || !timestamp || !obj) {
			throw new Error(`Invalid commit data ${JSON.stringify(msg.data)}`);
		}

		if (!indexers[msg.collection]) {
			throw new Error(`No indexer for collection ${msg.collection}`);
		}

		queues[msg.collection].add({ uri: new AtUri(uri), cid: CID.parse(cid), timestamp, obj });
	});

	setTimeout(async function processQueues() {
		await Promise.allSettled(collections.map(async (collection) => {
			const indexer = indexers[collection];
			if (!indexer) throw new Error(`No indexer for collection ${collection}`);

			const queue = queues[collection];
			if (!queue) throw new Error(`No queue for collection ${collection}`);

			const records = Array.from(queue);
			queue.clear();

			if (!records.length) return;

			console.time(`Writing records: ${records.length} for ${collection}`);
			await indexer.insertBulkRecords(records, { disableNotifs: true });
			console.timeEnd(`Writing records: ${records.length} for ${collection}`);
		}));
		setTimeout(processQueues, 1000);
	}, 1000);
}
