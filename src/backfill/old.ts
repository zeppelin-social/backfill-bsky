import { iterateAtpRepo } from "@atcute/car";
import {
	CredentialManager,
	type HeadersObject,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import cluster from "node:cluster";
import { cpus } from "node:os";
import { Agent, fetch as _fetch } from "undici";
import { AtUri } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import { WriteOpAction } from '@atproto/repo'
import type { CommitData } from './main.js'
import * as bsky from '@atproto/bsky'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { createClient } from '@redis/client'
import fs from 'node:fs'
import { fetchAllDids, sleep } from '../util/fetch.js'
import * as os from 'node:os'

type WorkerToMasterMessage = { type: "checkCooldown"; pds: string } | {
	type: "setCooldown";
	pds: string;
	until: number;
}
type MasterToWorkerMessage = { type: "cooldownResponse"; wait: boolean; waitTime?: number }

const cacheable = new CacheableLookup();

const agent = new Agent({
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
});

const fetch: typeof _fetch = (input, init) =>
	_fetch(input, init ? { ...init, dispatcher: agent } : {});

const date = () => `[${new Date().toISOString()}]`;
const _log = console.log.bind(console),
	_warn = console.warn.bind(console),
	_error = console.error.bind(console);
console.log = (...args) => _log(date(), ...args);
console.warn = (...args) => _warn(date(), ...args);
console.error = (...args) => _error(date(), ...args);

const repoQueue = new Queue("repo-processing", {
	removeOnSuccess: true,
	removeOnFailure: true,
});
const writeQueue = new Queue("write-commits", {
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

	writeQueue.process((job, done) => {
		const { out, did } = job.data;
		console.log(`Writing ${out.length} records for ${did}`);
		Promise.allSettled(out.map(({ uri, cid, indexedAt, record }) => indexingSvc.indexRecord(
			new AtUri(uri),
			CID.parse(cid),
			record,
			WriteOpAction.Create,
			indexedAt,
		)))
			.then(() => redis.sAdd("backfill:seen", did)).catch((err) =>
			console.error(`Error when writing ${ did }`, err)
		).finally(() => done(null));
	})
	
	async function main() {
		console.log("Reading DIDs");
		const repos = await readOrFetchDids();
		console.log(`Filtering out seen DIDs from ${repos.length} total`);
		const notSeen = await redis.smIsMember("backfill:seen", repos.map(repo => repo[0]))
			.then(seen => repos.filter((_, i) => !seen[i]));
		console.log(`Queuing ${notSeen.length} repos for processing`);
		for (const dids of batch(notSeen, 1000)) {
			const errors = await repoQueue.saveAll(dids.map(([did, pds ]) => repoQueue.createJob({ did, pds })))
			for (const [job, err] of errors) {
				console.error(`Failed to queue repo ${job.data.did}`, err);
			}
		}
		
		await new Promise<void>((resolve) => {
			const checkQueue = async () => {
				const jobCounts = await repoQueue.checkHealth();
				if (jobCounts.waiting === 0 && jobCounts.active === 0) {
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
	repoQueue.process(async (job) => {
		const { pds, did } = job.data;
		
		if (!pds || typeof pds !== "string" || !did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}
		
		const repo = await getRepo(pds, did as `did:${string}`);
		if (!repo) return;
		
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
			console.warn(`iterateAtpRepo error for did ${did} from pds ${pds} --- ${err}`);
		}
	});
	
	repoQueue.on("error", (err) => {
		console.error("Queue error:", err);
	});
	
	repoQueue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});
}

class WrappedRPC extends XRPC {
	constructor(public service: string) {
		// @ts-expect-error undici version mismatch causing fetch type incompatibility
		super({ handler: new CredentialManager({ service, fetch }) });
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
		return JSON.parse(fs.readFileSync('dids.json', 'utf-8'))
	} catch (err: any) {
		const dids = await fetchAllDids()
		writeDids(dids)
		return dids
	}
}

function writeDids(dids: Array<[string, string]>) {
	fs.writeFileSync('dids.json', JSON.stringify(dids))
}

function batch<T>(array: Array<T>, size: number): Array<Array<T>> {
	const result = []
	for (let i = 0; i < array.length; i += size) {
		result.push(array.slice(i, i + size))
	}
	return result
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
			const waitTime = ratelimitReset - now + 2000; // add 2s to be safe
			if (waitTime > 0) {
				await onRatelimit(waitTime);
			}
		}
	}
}
