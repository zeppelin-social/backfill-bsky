import { fromBytes } from "@atcute/cbor";
import { isBlob, isBytes, isCidLink, isLegacyBlob } from "@atcute/lexicons/interfaces";
import { MemoryCache } from "@atproto/identity";
import { BlobRef, lexToJson } from "@atproto/lexicon";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import { CID } from "multiformats/cid";
import fs from "node:fs/promises";
import { IdResolver, IndexingService } from "../indexingService.js";
import type { FromWorkerMessage } from "../main.js";
import type { CommitData } from "./repo.js";

export type ToInsertCommit = { uri: AtUri; cid: CID; timestamp: string; obj: unknown };

// 3 write workers, picked largely based on vibes to kind of evenly distribute load
export const writeWorkerAllocations = [[
	"app.bsky.feed.post",
	"chat.bsky.actor.declaration",
	"app.bsky.feed.postgate",
	"app.bsky.labeler.service",
	"app.bsky.feed.generator",
	"app.bsky.actor.status",
], [
	"app.bsky.feed.like",
	"app.bsky.actor.profile",
	"app.bsky.graph.list",
	"app.bsky.graph.block",
	"app.bsky.graph.starterpack",
	"app.bsky.graph.verification",
], [
	"app.bsky.feed.threadgate",
	"app.bsky.feed.repost",
	"app.bsky.graph.follow",
	"app.bsky.graph.listitem",
	"app.bsky.graph.listblock",
	"app.bsky.notification.declaration",
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
		poolIdleTimeoutMs: 30_000,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		fallbackPlc: process.env.FALLBACK_PLC_URL,
		// 1m stale, 2m max; very short because we should only really see any given identity once or twice
		didCache: new MemoryCache(1 * 60 * 1000, 2 * 60 * 1000),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	const batches: Record<string, ToInsertCommit[]> = {};

	let isShuttingDown = false;

	for (const collection of collections) {
		if (!indexingSvc.findIndexerForCollection(collection)) {
			throw new Error(`No indexer for collection ${collection}`);
		}
		batches[collection] = [];

		const queue = new Queue<{ commits: CommitData[] }>(`collection-write-${collection}`, {
			removeOnSuccess: true,
			removeOnFailure: true,
			isWorker: true,
		});

		queue.on("error", (err) => {
			console.error(`Queue error for collection ${collection}:`, err);
		});

		queue.on("failed", (_, err) => {
			console.error(`Job failed for collection ${collection}:`, err);
		});

		queue.process(10, async (job) => {
			if (isShuttingDown) return;
			const { commits } = job.data;

			for (const commit of commits) {
				const { did, path, cid: _cid, timestamp, obj: _obj } = commit;
				if (!did || !path || !_cid || !timestamp || !_obj) {
					throw new Error(`Invalid commit data ${JSON.stringify(commit)}`);
				}

				try {
					const uri = new AtUri(`at://${did}/${path}`);
					const cid = CID.parse(_cid);
					const obj = jsonToLex(_obj as Record<string, unknown>);

					batches[collection].push({ uri, cid, timestamp, obj });
				} catch (err) {
					console.error(`Error processing commit ${JSON.stringify(commit)}`, err);
				}
			}

			if (batches[collection].length > 100_000) {
				clearTimeout(queueTimer);
				await processQueue();
			}
		});
	}

	process.on("message", async (msg: { type: "shutdown" }) => {
		if (msg.type === "shutdown") {
			await handleShutdown();
			return;
		}
	});
	process.on("uncaughtException", (err) => {
		console.error(`Uncaught exception in worker ${workerIndex}`, err);
	});
	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	let queueTimer = setTimeout(processQueue, 1000);

	async function processQueue() {
		if (!isShuttingDown) {
			queueTimer = setTimeout(processQueue, 1000);
		}

		let recordCount = 0;
		const records = new Map<string, ToInsertCommit[]>();

		for (const collection in batches) {
			if (batches[collection].length > 0) {
				recordCount += batches[collection].length;
				records.set(collection, batches[collection]);
				batches[collection] = [];
			}
		}

		const time = `Writing ${recordCount} records by collection for ${collections.join(", ")}`;

		try {
			if (recordCount > 0) {
				console.time(time);
				await indexingSvc.bulkIndexToCollectionSpecificTables(
					records,
					// Validation is done by the repo worker; avoids @atproto/* version mismatch false negatives
					{ validate: false },
				);
				console.timeEnd(time);
			}
		} catch (err) {
			console.error(`Error processing queue for ${collections.join(", ")}`, err);
			await Promise.all(
				records.entries().map(([collection, recs]) =>
					fs.writeFile(
						`./failed-${collection}.jsonl`,
						recs.map((r) =>
							JSON.stringify({
								uri: r.uri.toString(),
								cid: r.cid.toString(),
								timestamp: r.timestamp,
								obj: lexToJson(r.obj),
							})
						).join("\n") + "\n",
						{ flag: "a" },
					)
				),
			);
			console.timeEnd(time);
		}
	}

	async function handleShutdown() {
		console.log(`Write collection worker ${workerIndex} received shutdown signal`);
		isShuttingDown = true;
		await processQueue();
		process.send?.({ type: "shutdownComplete" } satisfies FromWorkerMessage);
		process.exit(0);
	}
}

export function jsonToLex(val: Record<string, unknown>): unknown {
	try {
		if (!val) return val;
		// walk arrays
		if (Array.isArray(val)) {
			return val.map((item) => jsonToLex(item));
		}
		// objects
		if (typeof val === "object") {
			if (isCidLink(val)) {
				return CID.parse(val["$link"]);
			}
			if (isBytes(val)) {
				return fromBytes(val);
			}
			if (isLegacyBlob(val)) {
				return new BlobRef(CID.parse(val.cid), val.mimeType, -1, val);
			}
			if (isBlob(val)) {
				return new BlobRef(CID.parse(val.ref.$link), val.mimeType, val.size);
			}
			// walk plain objects
			const toReturn: Record<string, unknown> = {};
			for (const key of Object.keys(val)) {
				// @ts-expect-error — indexed access
				toReturn[key] = jsonToLex(val[key]);
			}
			return toReturn;
		}
	} catch {
		// pass through
	}
	return val;
}
