import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { createClient } from "@redis/client";
import RedisQueue from "bee-queue";
import * as fastq from "fastq";
import { CID } from "multiformats/cid";
import fs from "node:fs";
import readline from "node:readline/promises";
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

	// 100 concurrency queue to write records to the AppView database
	const writeQueue = new RedisQueue<CommitData>("write records", {
		removeOnSuccess: true,
		storeJobs: false,
		redis,
	});
	writeQueue.process(100, (job) => {
		const { uri, cid, indexedAt, record } = job.data;
		return indexingSvc.indexRecord(
			new AtUri(uri),
			CID.parse(cid),
			record,
			WriteOpAction.Create,
			indexedAt,
		);
	});

	// Uses worker threads to parse repos
	const workerPool = new WorkerPool<RepoWorkerMessage, Array<CommitData>>(
		new URL("./repoWorker.js", import.meta.url).href,
	);

	// 150 concurrency queue to fetch repos, queue for processing, then queue results for writing
	const getRepoQueue = fastq.promise(async ({ did, pds }: { did: string; pds: string }) => {
		console.log(`Fetching repo for ${did}`);
		const repoBytes = await getRepo(did, pds);
		if (!repoBytes?.length) return;
		workerPool.queueTask({ did, repoBytes }, (err, result) => {
			if (err) return console.error(`Error when processing ${did}`, err);
			console.log(`Writing ${result.length} records for ${did}`);
			return Promise.allSettled(result.map((commit) => writeQueue.createJob(commit).save()))
				.then(() => redis.sAdd("backfill:seen", did)).catch((err) =>
					console.error(`Error when writing ${did}`, err)
				);
		});
	}, 150);

	const repos = await readOrFetchDids();
	for (const [did, pds] of repos) {
		if (await redis.sIsMember("backfill:seen", did)) continue;
		getRepoQueue.push({ did, pds }).catch((err) =>
			console.error(`Error when fetching ${did}`, err)
		);
	}
}

void main();

const readOrFetchDids = async () => {
	try {
		const rs = fs.createReadStream("dids.json");
		const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
		const dids = new Map<string, string>();
		for await (const line of rl) {
			const [did, pds] = line.split(",");
			dids.set(did, pds);
		}
		return dids;
	} catch (err: any) {
		if (err.code !== "ENOENT") throw err;
		const dids = await fetchAllDids();
		writeDids(dids);
		return dids;
	}
};

const writeDids = (dids: Iterable<[string, string]>) => {
	fs.writeFileSync("dids.json", Array.from(dids).map(([did, pds]) => `${did},${pds}\n`).join(""));
};
