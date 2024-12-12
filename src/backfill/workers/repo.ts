import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import * as bsky from "@futuristick/atproto-bsky";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import { CID } from "multiformats/cid";
import * as shm from "shm-typed-array";

type CommitData = { uri: string; cid: string; indexedAt: string; record: unknown };

export async function repoWorker() {
	const queue = new Queue<{ did: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
	});

	const redis = createClient();
	await redis.connect();

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

	queue.process(5, async (job) => {
		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		const repo = shm.get(did, "Uint8Array");
		if (!repo?.byteLength) {
			console.warn(`Did not get repo for ${did}`);
			return;
		}

		try {
			console.time(`Processing repo: ${did}`);
			const commits: CommitData[] = [];
			const now = Date.now();
			for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
				const uri = `at://${did}/${collection}/${rkey}`;

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

				commits.push({
					uri,
					cid: cid.$link,
					indexedAt: new Date(indexedAt).toISOString(),
					record,
				});
			}
			console.timeEnd(`Processing repo: ${did}`);

			console.time(`Writing records: ${commits.length} for ${did}`);
			const insertHandle = indexingSvc.indexHandle(did, new Date().toISOString());
			const insertRecords = indexingSvc.db.transaction(async (txn) => {
				const indexingTx = indexingSvc.transact(txn);
				await Promise.allSettled(
					commits.map(async ({ uri: _uri, cid, indexedAt, record }) => {
						const uri = new AtUri(_uri);
						const indexer = indexingTx.findIndexerForCollection(uri.collection);
						if (indexer) {
							return indexer.insertRecord(uri, CID.parse(cid), record, indexedAt);
						}
					}),
				);
			});

			try {
				await Promise.allSettled([insertHandle, insertRecords]);
				await redis.sAdd("backfill:seen", did);
				console.timeEnd(`Writing records: ${commits.length} for ${did}`);
			} catch (err) {
				console.error(`Error when writing ${did}`, err);
			}
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
		} finally {
			shm.destroy(did);
		}
	});

	queue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});
}
