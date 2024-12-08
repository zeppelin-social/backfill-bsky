import { iterateAtpRepo } from "@atcute/car";
import {
	type HeadersObject,
	simpleFetchHandler,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import { CID } from "multiformats/cid";
import cluster from "node:cluster";
import fs from "node:fs";
import * as os from "node:os";
import SharedNodeBuffer from "shared-node-buffer";
import { Agent, setGlobalDispatcher } from "undici";
import { fetchAllDids, sleep } from "../util/fetch.js";
import type { CommitData } from "./main.js";

type WorkerToMasterMessage = { type: "checkCooldown"; pds: string } | {
	type: "setCooldown";
	pds: string;
	until: number;
};
type MasterToWorkerMessage = { type: "cooldownResponse"; wait: boolean; waitTime?: number };

const cacheable = new CacheableLookup();

setGlobalDispatcher(
	new Agent({
		keepAliveTimeout: 300_000,
		connect: {
			timeout: 300_000,
			lookup: (hostname, { family: _family, hints, all, ..._options }, callback) => {
				const family = !_family ? undefined : (_family === 6 || _family === "IPv6") ? 6 : 4;
				return cacheable.lookup(hostname, {
					..._options,
					...(family ? { family } : {}),
					...(hints ? { hints } : {}),
					...(all ? { all } : {}),
				}, callback);
			},
		},
	}),
);

const repoFetchingQueue = new Queue<{ did: string; pds: string }>("repo-fetching", {
	removeOnSuccess: true,
	removeOnFailure: true,
});
const repoProcessingQueue = new Queue<{ did: string; len: number }>("repo-processing", {
	removeOnSuccess: true,
	removeOnFailure: true,
});
const writeQueue = new Queue<{ out: CommitData[]; did: string }>("write-commits", {
	removeOnSuccess: true,
	removeOnFailure: true,
});

if (cluster.isPrimary) {
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

	const numCPUs = os.availableParallelism();
	const cooldowns = new Map<string, number>();

	cluster.on("message", (worker, message: WorkerToMasterMessage) => {
		switch (message.type) {
			case "checkCooldown": {
				const cooldownUntil = cooldowns.get(message.pds);
				const now = Date.now();
				if (cooldownUntil && cooldownUntil > now) {
					worker.send(
						{
							type: "cooldownResponse",
							wait: true,
							waitTime: cooldownUntil - now,
						} satisfies MasterToWorkerMessage,
					);
				} else {
					worker.send(
						{ type: "cooldownResponse", wait: false } satisfies MasterToWorkerMessage,
					);
				}
				break;
			}
			case "setCooldown": {
				cooldowns.set(message.pds, message.until);
				break;
			}
			default: {
				((_: never) => {})(message); // assert unreachable
				break;
			}
		}
	});

	for (let i = 0; i < numCPUs; i++) {
		cluster.fork();
	}

	cluster.on("exit", (worker, code, signal) => {
		console.error(`${worker.process.pid} died with code ${code} and signal ${signal}`);
		cluster.fork();
	});

	writeQueue.process(
		50,
		(
			job: Queue.Job<{ out: CommitData[]; did: string }>,
			done: (err: any, data?: any) => void,
		) => {
			const { out, did } = job.data;
			console.log(`Writing ${out.length} records for ${did}`);
			Promise.allSettled(out.map(({ uri, cid, indexedAt, record }) =>
				indexingSvc.indexRecord(
					new AtUri(uri),
					CID.parse(cid),
					record,
					WriteOpAction.Create,
					indexedAt,
				)
			)).then(() =>
				redis.sAdd("backfill:seen", did)
			).catch((err) => console.error(`Error when writing ${did}`, err)).finally(() =>
				done(null)
			);
		},
	);

	repoFetchingQueue.process(150, async (job) => {
		const { did, pds } = job.data;
		const repo = await getRepo(pds, did as `did:${string}`);
		if (repo) {
			const buffer = new SharedNodeBuffer(did, repo.byteLength);
			buffer.fill(repo);
			await repoProcessingQueue.createJob({ did, len: repo.byteLength }).setId(did).save();
		}
	});

	async function main() {
		console.log("Reading DIDs");
		const repos = await readOrFetchDids();
		console.log(`Filtering out seen DIDs from ${repos.length} total`);
		const notSeen = await redis.smIsMember("backfill:seen", repos.map((repo) => repo[0])).then((
			seen,
		) => repos.filter((_, i) => !seen[i]));
		console.log(`Queuing ${notSeen.length} repos for processing`);
		for (const dids of batch(notSeen, 1000)) {
			const errors = await repoFetchingQueue.saveAll(
				dids.map(([did, pds]) => repoFetchingQueue.createJob({ did, pds })),
			);
			for (const [job, err] of errors) {
				console.error(`Failed to queue repo ${job.data.did}`, err);
			}
		}

		await new Promise<void>((resolve) => {
			const checkQueue = async () => {
				const processJobCounts = await repoProcessingQueue.checkHealth();
				const fetchJobCounts = await repoFetchingQueue.checkHealth();
				if (
					processJobCounts.waiting === 0 && processJobCounts.active === 0
					&& fetchJobCounts.waiting === 0 && fetchJobCounts.active === 0
				) {
					resolve();
				} else {
					setTimeout(checkQueue, 1000);
				}
			};
			checkQueue();
		});
	}

	void main();
} else {
	repoProcessingQueue.process(async (job) => {
		const { did, len } = job.data;

		if (!did || typeof did !== "string" || !len || typeof len !== "number") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		const repo = new SharedNodeBuffer(did, len);

		try {
			const out = [];
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

				const commit = {
					uri,
					cid: cid.$link,
					indexedAt: new Date(indexedAt).toISOString(),
					record,
				} satisfies CommitData;
				out.push(commit);
			}

			const writeJob = writeQueue.createJob({ out, did });
			await writeJob.save();
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
		}
	});

	repoProcessingQueue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	repoProcessingQueue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});
}

class WrappedRPC extends XRPC {
	constructor(public service: string) {
		super({ handler: simpleFetchHandler({ service }) });
	}

	override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
		const url = new URL("/xrpc/" + options.nsid, this.service).href;

		if (!cluster.isPrimary) {
			await new Promise<void>((resolve) => {
				if (!process.send) return;
				process.send(
					{ type: "checkCooldown", pds: this.service } satisfies WorkerToMasterMessage,
				);

				const handler = (message: MasterToWorkerMessage) => {
					if (message.type === "cooldownResponse") {
						process.off("message", handler);
						if (message.wait && message.waitTime) {
							sleep(message.waitTime + 1000).then(() => resolve());
						} else {
							resolve();
						}
					}
				};

				process.on("message", handler);
			});
		}

		const request = async () => {
			const res = await super.request(options);

			if (!cluster.isPrimary) {
				await processRatelimitHeaders(res.headers, url, (wait) => {
					process.send!(
						{
							type: "setCooldown",
							pds: this.service,
							until: Date.now() + wait,
						} satisfies WorkerToMasterMessage,
					);
				});
			}

			return res;
		};

		try {
			return await request();
		} catch (err) {
			if (attempt > 6) throw err;

			if (err instanceof XRPCError) {
				if (err.status === 429) {
					if (!cluster.isPrimary) {
						await processRatelimitHeaders(err.headers, url, sleep);
					}
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

async function getRepo(pds: string, did: `did:${string}`) {
	const rpc = new WrappedRPC(pds);
	try {
		const { data } = await rpc.get("com.atproto.sync.getRepo", { params: { did } });
		return data;
	} catch (err) {
		console.error(`getRepo error for did ${did} from pds ${pds} --- ${err}`);
	}
}

async function readOrFetchDids(): Promise<Array<[string, string]>> {
	try {
		return JSON.parse(fs.readFileSync("dids.json", "utf-8"));
	} catch (err: any) {
		const dids = await fetchAllDids();
		writeDids(dids);
		return dids;
	}
}

function writeDids(dids: Array<[string, string]>) {
	fs.writeFileSync("dids.json", JSON.stringify(dids));
}

function batch<T>(array: Array<T>, size: number): Array<Array<T>> {
	const result = [];
	for (let i = 0; i < array.length; i += size) {
		result.push(array.slice(i, i + size));
	}
	return result;
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
				await onRatelimit(waitTime);
			}
		}
	}
}
