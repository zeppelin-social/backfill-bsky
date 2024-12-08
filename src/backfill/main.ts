import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { createClient } from "@redis/client";
import RedisQueue from "bee-queue";
import { CID } from "multiformats/cid";
import fs from "node:fs";
import { fetchAllDids, getRepo } from "../util/fetch.js";
import { WorkerPool } from "../util/workerPool.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_DB_POSTGRES_URL: string;
			BSKY_DB_POSTGRES_SCHEMA: string;
			BSKY_REPO_PROVIDER: string;
			BSKY_DID_PLC_URL: string;
		}
	}
}

for (
	const envVar of [
		"BSKY_DB_POSTGRES_URL",
		"BSKY_DB_POSTGRES_SCHEMA",
		"BSKY_REPO_PROVIDER",
		"BSKY_DID_PLC_URL",
	]
) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

export type RepoWorkerMessage = { did: string; repoBytes: Uint8Array };
export type CommitData = { uri: string; cid: string; indexedAt: string; record: unknown };

async function main() {
	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 50,
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

	const redis = createClient();
	await redis.connect();

	// Uses worker threads to parse repos
	const workerPool = new WorkerPool<RepoWorkerMessage, Array<CommitData>>(
		new URL("./repoWorker.js", import.meta.url).href,
	);

	// 200 concurrency queue to fetch repos, queue for processing, then write results
	const getRepoQueue = new RedisQueue<{ did: string; pds: string }>(
		"getRepo",
		{ redis, removeOnSuccess: true, storeJobs: false }
	);
	getRepoQueue.process(200, async job => {
		const { did, pds } = job.data;
		console.log(`Fetching repo for ${did}`);
		const repoBytes = await getRepo(did, pds);
		if (!repoBytes?.length) return;
		workerPool.queueTask({ did, repoBytes }, (err, result) => {
			if (err) return console.error(`Error when processing ${did}`, err);
			console.log(`Writing ${result.length} records for ${did}`);
			return Promise.all(result.map(({ uri, cid, indexedAt, record }) => indexingSvc.indexRecord(
				new AtUri(uri),
				CID.parse(cid),
				record,
				WriteOpAction.Create,
				indexedAt,
			)))
				.then(() => redis.sAdd("backfill:seen", did)).catch((err) =>
					console.error(`Error when writing ${did}`, err)
				);
		});
	})
	
	const repos = await readOrFetchDids();
	const notSeen = await redis.smIsMember("backfill:seen", repos.map(repo => repo[0]))
		.then(seen => repos.filter((_, i) => !seen[i]));
	for (const dids of batch(notSeen, 1000)) {
		const errors = await getRepoQueue.saveAll(dids.map(([did, pds ]) => getRepoQueue.createJob({ did, pds })))
		for (const [job, err] of errors) {
			console.error(`Failed to queue repo ${job.data.did}`, err);
		}
	}
}

void main();

const readOrFetchDids = async (): Promise<Array<[string, string]>> => {
	try {
		return JSON.parse(fs.readFileSync("dids.json", "utf-8"));
	} catch (err: any) {
		const dids = await fetchAllDids();
		writeDids(dids);
		return dids;
	}
};

const writeDids = (dids: Array<[string, string]>) => {
	fs.writeFileSync("dids.json", JSON.stringify(dids));
};

const batch = <T>(array: Array<T>, size: number): Array<Array<T>> => {
	const result = [];
	for (let i = 0; i < array.length; i += size) {
		result.push(array.slice(i, i + size));
	}
	return result;
}
