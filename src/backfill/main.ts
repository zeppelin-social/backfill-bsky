import {
	type HeadersObject,
	simpleFetchHandler,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import * as bsky from "@futuristick/atproto-bsky";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import { pack, unpack } from "msgpackr";
import cluster from "node:cluster";
import fs from "node:fs/promises";
import * as os from "node:os";
import path from "node:path";
import PQueue from "p-queue";
import { Agent, RetryAgent, setGlobalDispatcher } from "undici";
import { fetchAllDids, sleep } from "../util/fetch.js";
import { type CommitMessage, repoWorker } from "./workers/repo.js";
import { writeWorker, writeWorkerAllocations } from "./workers/write.js";

type WorkerMessage = CommitMessage;

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

const DB_SETTINGS = { archive_mode: "off", wal_level: "minimal", max_wal_senders: 0, fsync: "off" };

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

const PENDING_SENDS: Record<string, number> = {};

if (cluster.isPrimary) {
	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 50,
	});

	await Promise.all(
		Object.entries(DB_SETTINGS).map(([setting, value]) =>
			db.pool.query(`ALTER SYSTEM SET ${setting} = ${value}`)
		),
	);

	const indexes = await db.pool.query(`
		SELECT pg_get_indexdef(i.indexrelid) AS createcmd,
			   'DROP INDEX ' || i.indexrelid::regclass AS dropcmd
		FROM pg_index i
		JOIN pg_class cl ON cl.oid = i.indexrelid
		LEFT JOIN pg_constraint co ON co.conindid = i.indexrelid
		WHERE cl.relname LIKE '%_idx'
		AND co.conindid IS NULL
		`);

	await Promise.all(indexes.rows.map(({ dropcmd }) => db.pool.query(dropcmd)));

	const redis = createClient();
	await redis.connect();

	const REPOS_DIR = await fs.mkdtemp(path.join(os.tmpdir(), "backfill-bsky-repos-"));

	const pidToWorkerInfo = new Map<number, { kind: "repo" } | { kind: "write"; index: number }>();
	const collectionToWriteWorkerId = new Map<string, number>();

	// Initialize write workers and track which collections they're responsible for
	const spawnWriteWorker = (i: number) => {
		const worker = cluster.fork({ WORKER_KIND: "write", WORKER_INDEX: `${i}` });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		pidToWorkerInfo.set(worker.process.pid, { kind: "write", index: i });
		for (const collection of writeWorkerAllocations[i]) {
			collectionToWriteWorkerId.set(collection, worker.id);
		}
		worker.on("error", (err) => {
			console.error(`Write worker error: ${err}`);
			worker.kill();
			cluster.fork();
		});
	};
	for (let i = 0; i < 3; i++) {
		spawnWriteWorker(i);
	}

	// Initialize repo workers
	const spawnRepoWorker = () => {
		const worker = cluster.fork({ WORKER_KIND: "repo", REPOS_DIR });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		pidToWorkerInfo.set(worker.process.pid, { kind: "repo" });
		worker.on("error", (err) => {
			console.error(`Repo worker error: ${err}`);
			worker.kill();
			cluster.fork();
		});
	};

	const numCPUs = os.availableParallelism();
	for (let i = 3; i < numCPUs * 3; i++) {
		spawnRepoWorker();
	}

	cluster.on("exit", ({ process: { pid } }, code, signal) => {
		console.warn(`Worker ${pid} exited with code ${code} and signal ${signal}`);
		const workerInfo = pidToWorkerInfo.get(pid ?? -1);
		if (!workerInfo || pid === undefined) {
			console.error(`Unknown worker exited with code ${code} and signal ${signal}`);
			// Should we restart a worker that should never exist?
			// I don't think this is likely to ever be reached
			cluster.fork();
		} else {
			pidToWorkerInfo.delete(pid);
			if (workerInfo.kind === "write") {
				spawnWriteWorker(workerInfo.index);
			} else if (workerInfo.kind === "repo") {
				spawnRepoWorker();
			} else {
				throw new Error(
					`Unknown worker kind: ${
						JSON.stringify(workerInfo)
					} exited with code ${code} and signal ${signal}`,
				);
			}
		}
	});

	cluster.on("message", (_, message: WorkerMessage) => {
		if (message?.type !== "commit" || !message.collection || !message.commits) {
			throw new Error(`Received invalid worker message: ${JSON.stringify(message)}`);
		}
		if (!message.commits.length) return;

		const writeWorkerId = collectionToWriteWorkerId.get(message.collection);
		// Repos can contain non-Bluesky records, just ignore them
		if (writeWorkerId === undefined) return;
		const writeWorker = cluster.workers?.[writeWorkerId];
		if (!writeWorker) {
			throw new Error(`Could not find write worker for collection ${message.collection}`);
		}

		PENDING_SENDS[message.collection] = (PENDING_SENDS[message.collection] || 0) + 1;
		writeWorker.send(message, () => {
			PENDING_SENDS[message.collection]--;
			if (PENDING_SENDS[message.collection] < 0) PENDING_SENDS[message.collection] = 0;
		});
	});

	process.on("beforeExit", async () => {
		console.log("Resetting DB settings");
		await Promise.all(
			Object.keys(DB_SETTINGS).map((setting) =>
				db.pool.query(`ALTER SYSTEM RESET ${setting}`)
			),
		);

		// console.log("Recreating indexes");
		// await Promise.all(indexes.rows.map(({ createcmd }) => db.pool.query(createcmd)));

		console.log("Closing DB connections");
		await db.pool.end();
		await redis.disconnect();
	});

	// Aim to be fetching ~100 repos at a time
	const fetchQueue = new PQueue({ concurrency: 100 });

	async function main() {
		console.log("Reading DIDs");
		const repos = await readOrFetchDids();
		console.log(`Filtering out seen DIDs from ${repos.length} total`);

		const seenDids = new Set(await redis.sMembers("backfill:seen"));

		setInterval(() => {
			console.log(`> PENDING SENDS: ${JSON.stringify(PENDING_SENDS)}`);
		}, 1000);

		console.log(`Queuing ${repos.length} repos for processing`);
		for (const [did, pds] of repos) {
			// dumb pds doesn't implement getRepo
			if (pds.includes("blueski.social")) continue;
			// This may be faster as a single set difference?
			if (seenDids.has(did)) continue;
			// Wait for queue to be below 250 before adding another job
			await fetchQueue.onSizeLessThan(250);
			void fetchQueue.add(() => queueRepo(pds, did)).catch((e) =>
				console.error(`Error queuing repo for ${did} `, e)
			);
		}
	}

	void main();

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
				await Bun.write(path.join(REPOS_DIR, did), repo);
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
			return unpack(await fs.readFile("dids.cache"), { lazy: true });
		} catch (err: any) {
			const dids = await fetchAllDids();
			await writeDids(dids);
			return dids;
		}
	}

	function writeDids(dids: Array<[string, string]>) {
		return fs.writeFile("dids.cache", pack(dids));
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
} else {
	if (process.env.WORKER_KIND === "repo") {
		void repoWorker();
	} else if (process.env.WORKER_KIND === "write") {
		void writeWorker();
	} else {
		throw new Error(`Unknown worker kind: ${process.env.WORKER_KIND}`);
	}
}
