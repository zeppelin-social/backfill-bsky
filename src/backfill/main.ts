import {
	type HeadersObject,
	simpleFetchHandler,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import { pack, unpack } from "msgpackr";
import cluster from "node:cluster";
import fs from "node:fs";
import * as os from "node:os";
import PQueue from "p-queue";
import * as shm from "shm-typed-array";
import { Agent, RetryAgent, setGlobalDispatcher } from "undici";
import { fetchAllDids, sleep } from "../util/fetch.js";
import { repoWorker } from "./workers/repo.js";

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

const cacheable = new CacheableLookup();

setGlobalDispatcher(
	new RetryAgent(
		new Agent({
			keepAliveTimeout: 30_000,
			connect: {
				timeout: 30_000,
				lookup: (hostname, { family: _family, hints, all, ..._options }, callback) => {
					const family = !_family
						? undefined
						: (_family === 6 || _family === "IPv6")
						? 6
						: 4;
					return cacheable.lookup(hostname, {
						..._options,
						...(family ? { family } : {}),
						...(hints ? { hints } : {}),
						...(all ? { all } : {}),
					}, callback);
				},
			},
		}),
		{
			errorCodes: [
				"ECONNRESET",
				"ECONNREFUSED",
				"ETIMEDOUT",
				"ENETDOWN",
				"ENETUNREACH",
				"EHOSTDOWN",
				"UND_ERR_SOCKET",
			],
		},
	),
);

const queue = new Queue<{ did: string }>("repo-processing", {
	removeOnSuccess: true,
	removeOnFailure: true,
});

const redis = createClient();
await redis.connect();

if (cluster.isPrimary) {
	const numCPUs = os.availableParallelism();

	for (let i = 0; i < numCPUs; i++) {
		cluster.fork();
	}

	cluster.on("exit", (worker, code, signal) => {
		console.error(`${worker.process.pid} died with code ${code} and signal ${signal}`);
		cluster.fork();
	});

	const fetchQueue = new PQueue({ concurrency: 100 });

	async function main() {
		console.log("Reading DIDs");
		const repos = await readOrFetchDids();
		console.log(`Filtering out seen DIDs from ${repos.length} total`);

		const seenDids = new Set(await redis.sMembers("backfill:seen"));

		console.log(`Queuing ${repos.length} repos for processing`);
		for (const [did, pds] of repos) {
			if (seenDids.has(did)) continue;
			await fetchQueue.onSizeLessThan(100);
			void fetchQueue.add(() => queueRepo(pds, did)).catch((e) =>
				console.error(`Error queuing repo for ${did} `, e)
			);
		}
	}

	void main();
} else {
	void repoWorker();
}

class WrappedRPC extends XRPC {
	constructor(public service: string) {
		super({ handler: simpleFetchHandler({ service }) });
	}

	override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
		const url = new URL("/xrpc/" + options.nsid, this.service).href;

		const request = async () => {
			const res = await super.request(options);
			await processRatelimitHeaders(res.headers, url, sleep);
			return res;
		};

		try {
			return await request();
		} catch (err) {
			if (attempt > 6) throw err;

			if (err instanceof XRPCError) {
				if (err.status === 429) {
					await processRatelimitHeaders(err.headers, url, sleep);
				} else throw err;
			} else if (err instanceof TypeError) {
				console.warn(`fetch failed for ${url}, skipping`);
				throw err;
			} else {
				await sleep(backoffs[attempt] || 60000);
			}
			console.warn(`Retrying request to ${url}, on attempt ${attempt}`);
			return this.request(options, attempt + 1);
		}
	}
}

async function queueRepo(pds: string, did: string) {
	console.time(`Fetching repo: ${did}`);
	try {
		const rpc = new WrappedRPC(pds);
		const { data: repo } = await rpc.get("com.atproto.sync.getRepo", {
			params: { did: did as `did:${string}` },
		});
		if (repo?.length) {
			const shared = shm.create(repo.length, "Uint8Array", did);
			if (shared) shared.set(repo);
			await queue.createJob({ did }).setId(did).save();
		}
	} catch (err) {
		console.error(`Error fetching repo for ${did} --- ${err}`);
		if (err instanceof XRPCError) {
			if (
				err.name === "RepoDeactivated" || err.name === "RepoTakendown"
				|| err.name === "RepoNotFound"
			) {
				await redis.sAdd("backfill:seen", did);
			}
		}
	} finally {
		console.timeEnd(`Fetching repo: ${did}`);
	}
}

async function readOrFetchDids(): Promise<Array<[string, string]>> {
	try {
		return unpack(fs.readFileSync("dids.cache"));
	} catch (err: any) {
		const dids = await fetchAllDids();
		writeDids(dids);
		return dids;
	}
}

function writeDids(dids: Array<[string, string]>) {
	fs.writeFileSync("dids.cache", pack(dids));
}

const backoffs = [1_000, 5_000, 15_000, 30_000, 60_000, 120_000, 300_000];

async function processRatelimitHeaders(
	headers: HeadersObject,
	url: string,
	onRatelimit: (wait: number) => unknown,
) {
	const remainingHeader = headers["ratelimit-remaining"],
		resetHeader = headers["ratelimit-reset"];
	if (!remainingHeader || !resetHeader) return;

	const ratelimitRemaining = parseInt(remainingHeader);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
		const ratelimitReset = parseInt(resetHeader) * 1000;
		if (isNaN(ratelimitReset)) {
			console.error("ratelimit-reset header is not a number at url " + url);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 1000; // add 1s to be safe
			if (waitTime > 0) {
				console.log("Rate limited at " + url + ", waiting " + waitTime + "ms");
				await onRatelimit(waitTime);
			}
		}
	}
}
