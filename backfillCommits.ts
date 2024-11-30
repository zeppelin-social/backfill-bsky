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
import * as fs from "node:fs";
import { cpus } from "node:os";
import { Agent, fetch as _fetch } from "undici";
import { type BackfillLine, batch, getPdses, sleep } from "./shared.js";

type WorkerToMasterMessage = { type: "checkCooldown"; pds: string } | {
	type: "setCooldown";
	pds: string;
	until: number;
} | {
	type: "checkDid";
	pds: string;
	did: string;
}

type MasterToWorkerMessage = { type: "cooldownResponse"; wait: boolean; waitTime?: number } | { type: "didResponse"; seen: boolean };

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
	const numCPUs = cpus().length;
	const cooldowns = new Map<string, number>();

	const ws = fs.createWriteStream("backfill-unsorted.jsonl", { flags: "a+" });

	let seenDids: Record<string, Record<string, boolean>>;
	try {
		seenDids = JSON.parse(fs.readFileSync("seen-dids.json", "utf-8"));
	} catch {
		if (fs.existsSync("seen-dids.json")) {
			fs.copyFileSync("seen-dids.json", "seen-dids.json.bak");
		}
		seenDids = {};
	}

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
			case 'checkDid': {
				const seen = seenDids[message.pds]?.[message.did];
				worker.send({ type: 'didResponse', seen } satisfies MasterToWorkerMessage);
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

	setInterval(() => {
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	}, 20_000);

	const onFinish = async () => {
		ws.close();
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
		await repoQueue.close();
		await writeQueue.close();
	};

	process.on("SIGINT", onFinish);
	process.on("SIGTERM", onFinish);
	process.on("exit", onFinish);
	
	writeQueue.process((job, done) => {
		const { lines, pds, did } = job.data;
		if (!pds || !did || !lines || typeof lines !== "string") console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
		ws.write(lines, err => {
			if (err) console.error(`Error writing ${did} commits: ${err}`);
			else seenDids[pds][did] = true;
			done();
		});
	})

	async function main() {
		const pdses = await getPdses();
		
		await Promise.allSettled(pdses.map(async (pds) => {
			try {
				for await (const dids of batch(listRepos(pds), 100)) {
					await repoQueue.saveAll(
						dids.filter((did) => !seenDids[pds][did]).map((did) => {
							const job = repoQueue.createJob({ pds, did });
							job.setId(did);
							return job;
						}),
					);
				}
				console.log(`Finished queueing ${pds}`);
			} catch (err) {
				console.warn(`Unknown error, skipping pds ${pds} --- ${err}`);
			}
		}));

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

		await onFinish();
	}

	void main();
} else {
	repoQueue.process(async (job) => {
		const { pds, did } = job.data;

		if (!pds || typeof pds !== "string" || !did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}
		
		const seen = await checkSeen(pds, did);
		if (seen) return;
		
		const repo = await getRepo(pds, did as `did:${string}`);
		if (!repo) return;

		let lines = "";

		try {
			for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
				const uri = `at://${did}/${collection}/${rkey}`;
				let timestamp: number;
				try {
					timestamp = parseTID(rkey).timestamp;
				} catch {
					timestamp =
						record && typeof record === "object" && "createdAt" in record
							&& typeof record.createdAt === "string"
							? new Date(record.createdAt).getTime()
							: Date.now();
				}
				const line: BackfillLine = {
					action: "create",
					timestamp,
					uri,
					cid: cid.$link,
					record,
				};

				lines += JSON.stringify(line) + "\n";
			}
			
			const writeJob = writeQueue.createJob({ lines, pds, did });
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
	
	function checkSeen(pds: string, did: string) {
		return new Promise<boolean | null>((resolve, reject) => {
			if (!process.send) return reject(new Error("Not in cluster"));
			const handler = (message: MasterToWorkerMessage) => {
				if (message.type === "didResponse") {
					process.off("message", handler);
					resolve(message.seen);
				}
			};
			
			process.on("message", handler);
			process.send({ type: "checkDid", pds, did } satisfies WorkerToMasterMessage);
		});
	}
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

async function* listRepos(pds: string) {
	let cursor: string | undefined = "";
	const rpc = new WrappedRPC(pds);
	do {
		try {
			console.log(`Listing repos for pds ${pds} at cursor ${cursor}`);
			const { data: { repos, cursor: newCursor } } = await rpc.get(
				"com.atproto.sync.listRepos",
				{ params: { limit: 1000, cursor } },
			);
			
			cursor = newCursor as string | undefined;
			
			for (const repo of repos) {
				yield repo.did;
			}
		} catch (err) {
			console.error(`listRepos error for pds ${pds} at cursor ${cursor} --- ${err}`);
			return;
		}
	} while (cursor);
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
