import {
	type HeadersObject,
	simpleFetchHandler,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { createClient } from "@redis/client";
import * as bsky from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import cluster, { type Worker } from "node:cluster";
import fs from "node:fs/promises";
import * as os from "node:os";
import path from "node:path";
import PQueue from "p-queue";
import { Agent, RetryAgent, setGlobalDispatcher } from "undici";
import { fetchAllDids, sleep } from "./util/fetch.js";
import { openSearchWorker } from "./workers/opensearch.js";
import { type CommitMessage, repoWorker } from "./workers/repo.js";
import { writeCollectionWorker, writeWorkerAllocations } from "./workers/writeCollection.js";
import { writeRecordWorker } from "./workers/writeRecord.js";

type WorkerMessage = CommitMessage | { type: "shutdownComplete" };

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_DB_POSTGRES_URL: string;
			BSKY_DB_POSTGRES_SCHEMA: string;
			BSKY_DID_PLC_URL: string;
			FALLBACK_PLC_URL?: string;
			OPENSEARCH_URL: string;
			OPENSEARCH_USERNAME: string;
			OPENSEARCH_PASSWORD: string;
		}
	}
}

for (const envVar of ["BSKY_DB_POSTGRES_URL", "BSKY_DB_POSTGRES_SCHEMA", "BSKY_DID_PLC_URL"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

for (
	const envVar of [
		"FALLBACK_PLC_URL",
		"OPENSEARCH_URL",
		"OPENSEARCH_USERNAME",
		"OPENSEARCH_PASSWORD",
	]
) {
	if (!process.env[envVar]) console.warn(`Missing optional env var ${envVar}`);
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
						: _family === 6 || _family === "IPv6"
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

if (cluster.isWorker) {
	if (process.env.WORKER_KIND === "repo") {
		void repoWorker();
	} else if (process.env.WORKER_KIND === "writeCollection") {
		void writeCollectionWorker();
	} else if (process.env.WORKER_KIND === "writeRecord") {
		void writeRecordWorker();
	} else if (process.env.WORKER_KIND === "opensearch") {
		void openSearchWorker();
	} else {
		throw new Error(`Unknown worker kind: ${process.env.WORKER_KIND}`);
	}
} else {
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

	const redis = createClient();
	await redis.connect();

	const queue = new Queue<{ did: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
	});

	const REPOS_DIR = await fs.mkdtemp(path.join(os.tmpdir(), "backfill-bsky-repos-"));

	const workers = {
		repo: {},
		writeCollection: {},
		writeRecord: { pid: 0, id: 0 },
		writeRecord2: { pid: 0, id: 0 },
		openSearch: { pid: 0, id: 0 },
	} as {
		repo: Record<number, { kind: "repo" }>;
		writeCollection: Record<number, { kind: "writeCollection"; index: number }>;
		writeRecord: { pid: number; id: number };
		writeRecord2: { pid: number; id: number };
		openSearch?: { pid: number; id: number };
	};

	const collectionToWriteWorkerId = new Map<string, number>();

	// Initialize write workers and track which collections they're responsible for
	const spawnWriteCollectionWorker = (i: number) => {
		const worker = cluster.fork({ WORKER_KIND: "writeCollection", WORKER_INDEX: `${i}` });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		workers.writeCollection[worker.process.pid] = { kind: "writeCollection", index: i };
		for (const collection of writeWorkerAllocations[i]) {
			collectionToWriteWorkerId.set(collection, worker.id);
		}
		worker.on("error", (err) => {
			console.error(`Write collection worker error: ${err}`);
			worker.kill();
			cluster.fork({ WORKER_KIND: "writeCollection", WORKER_INDEX: `${i}` });
		});
	};
	for (let i = 0; i < 3; i++) {
		spawnWriteCollectionWorker(i);
	}

	const spawnWriteRecordWorker = () => {
		const worker = cluster.fork({ WORKER_KIND: "writeRecord" });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		workers.writeRecord.pid = worker.process.pid;
		workers.writeRecord.id = worker.id;
		worker.on("error", (err) => {
			console.error(`Write record worker error: ${err}`);
			worker.kill();
			cluster.fork({ WORKER_KIND: "writeRecord" });
		});

		const worker2 = cluster.fork({ WORKER_KIND: "writeRecord" });
		if (!worker2.process?.pid) throw new Error("Worker process not found");
		workers.writeRecord2.pid = worker2.process.pid;
		workers.writeRecord2.id = worker2.id;
		worker2.on("error", (err) => {
			console.error(`Write record worker error: ${err}`);
			worker2.kill();
			cluster.fork({ WORKER_KIND: "writeRecord" });
		});
	};

	if (!workers.writeRecord.pid) {
		spawnWriteRecordWorker();
	}

	// Initialize repo workers
	const spawnRepoWorker = () => {
		const worker = cluster.fork({ WORKER_KIND: "repo", REPOS_DIR });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		workers.repo[worker.process.pid] = { kind: "repo" };
		worker.on("error", (err) => {
			console.error(`Repo worker error: ${err}`);
			worker.kill();
			cluster.fork({ WORKER_KIND: "repo", REPOS_DIR });
		});
	};

	const numCPUs = os.availableParallelism();
	const repoWorkerCount = numCPUs <= 10 ? 32 : numCPUs >= 32 ? 96 : numCPUs * 3;
	for (let i = 3; i < repoWorkerCount; i++) {
		spawnRepoWorker();
	}

	// Initialize OpenSearch worker
	const spawnOpenSearchWorker = () => {
		if (
			!process.env.OPENSEARCH_URL || !process.env.OPENSEARCH_USERNAME
			|| !process.env.OPENSEARCH_PASSWORD
		) return;

		const worker = cluster.fork({ WORKER_KIND: "opensearch" });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		workers.openSearch!.pid = worker.process.pid;
		workers.openSearch!.id = worker.id;
		worker.on("error", (err) => {
			console.error(`OpenSearch worker error: ${err}`);
			worker.kill();
			cluster.fork({ WORKER_KIND: "opensearch" });
		});
	};
	spawnOpenSearchWorker();

	let isShuttingDown = false;

	cluster.on("exit", handleWorkerExit);

	cluster.on("message", forwardMessageToWorkers);

	process.on("beforeExit", async () => {
		console.log("Resetting DB settings");
		await Promise.all(
			Object.keys(DB_SETTINGS).map((setting) =>
				db.pool.query(`ALTER SYSTEM RESET ${setting}`)
			),
		);

		console.log("Closing DB connections");
		await db.pool.end();
		await redis.disconnect();
	});

	process.on("exit", (code) => {
		console.log(`Exiting with code ${code}`);
	});

	const fetchQueue = new PQueue({ concurrency: 1_000 });

	process.on("SIGINT", async () => {
		console.log("\nReceived SIGINT. Starting graceful shutdown...");
		isShuttingDown = true;

		// Stop accepting new repos
		fetchQueue.pause();
		pdsQueues.forEach((queue) => queue.pause());

		// Track which workers have completed
		const completedWorkers = new Set<number>();

		// Set up completion message handler
		cluster.on("message", (worker, msg: { type?: string }) => {
			if (msg.type === "shutdownComplete") {
				console.log(`Worker ${worker.id} completed shutdown`);
				completedWorkers.add(worker.id);
			}
		});

		const writeWorkerIds = [
			...new Set(collectionToWriteWorkerId.values()),
			workers.writeRecord.id,
			workers.writeRecord2.id,
		];

		console.log("Waiting for write workers to finish...");

		for (const workerId of writeWorkerIds) {
			const worker = cluster.workers?.[workerId];
			if (!worker) continue;
			worker.send({ type: "shutdown" });
		}

		if (workers.openSearch) {
			cluster.workers?.[workers.openSearch.id]?.send({ type: "shutdown" });
		}

		// Wait for all workers to report completion or timeout
		const timeoutPromise = new Promise((resolve) => setTimeout(resolve, 60_000));
		const completionPromise = new Promise((resolve) => {
			const checkInterval = setInterval(() => {
				if (writeWorkerIds.every((id) => completedWorkers.has(id))) {
					clearInterval(checkInterval);
					resolve(true);
				}
			}, 100);
		});

		await Promise.race([timeoutPromise, completionPromise]);

		console.log("Shutting down...");
		process.exit(0);
	});

	let totalProcessed = 0, fetchedOverInterval = 0;

	setInterval(async () => {
		const newTotalProcessed = await redis.sCard("backfill:seen");

		const processed = (newTotalProcessed - totalProcessed) / 5,
			fetched = fetchedOverInterval / 5;
		fetchedOverInterval = 0;

		console.log(
			`Processed repos: ${processed.toFixed(1)}/s | Fetched repos: ${fetched.toFixed(1)}/s`,
			`\n`,
			`Fetch queue: ${fetchQueue.size} DIDs | ${fetchQueue.pending} pending`,
		);
	}, 5_000);

	setTimeout(function forceGC() {
		Bun.gc(true);
		setTimeout(forceGC, 30_000);
	}, 30_000);

	async function main() {
		console.log("Reading DIDs");
		const seenDids = new Set(await redis.sMembers("backfill:seen"));
		console.log(`Seen: ${seenDids.size} DIDs`);
		totalProcessed = seenDids.size;

		await new Promise<void>(async (resolve) => {
			await fetchAllDids(async (did, pds) => {
				if (isShuttingDown) resolve();
				// dumb pds doesn't implement getRepo
				if (pds.includes("blueski.social")) return;
				if (seenDids.has(did)) return;
				await fetchQueue.onSizeLessThan(10_000);
				void fetchQueue.add(() => queueRepo(pds, did)).catch((e) =>
					console.error(`Error queuing repo for ${did} `, e)
				);
			});
		});
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

	const pdsQueues = new Map<string, PQueue>();

	async function queueRepo(pds: string, did: string) {
		let pdsQueue = pdsQueues.get(pds);
		if (!pdsQueue) {
			pdsQueue = new PQueue({ concurrency: 25 });
			pdsQueues.set(pds, pdsQueue);
		}

		await pdsQueue.add(async () => {
			try {
				const rpc = new WrappedRPC(pds);
				const { data: repo } = await rpc.get("com.atproto.sync.getRepo", {
					params: { did: did as `did:${string}` },
				});
				if (repo?.length) {
					await Bun.write(path.join(REPOS_DIR, did), repo);
					await queue.createJob({ did }).setId(did).save();
					fetchedOverInterval++;
				}
			} catch (err) {
				if (
					["RepoDeactivated", "RepoTakendown", "RepoNotFound", "NotFound"].some((s) =>
						`${err}`.includes(s)
					)
				) {
					await redis.sAdd("backfill:seen", did);
					console.error(`Marking ${did} as seen --- ${err}`);
				} else {
					console.error(`Error fetching repo for ${did} --- ${err}`);
				}
			}
		});
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

	const failedMessages = new Set<WorkerMessage>();
	function forwardMessageToWorkers(worker: Worker, message: WorkerMessage) {
		if (message.type === "shutdownComplete") {
			if (!isShuttingDown) handleWorkerExit(worker, 0, "SIGINT");
			if (!worker.process.killed) worker.kill();
			return;
		}

		if (message?.type !== "commit" || !message.collection || !message.commits) {
			throw new Error(`Received invalid worker message: ${JSON.stringify(message)}`);
		}
		if (!message.commits.length) return;

		const writeCollectionWorkerId = collectionToWriteWorkerId.get(message.collection);
		// Repos can contain non-Bluesky records, just ignore them
		if (writeCollectionWorkerId === undefined) {
			console.warn(`Received commit for unknown collection ${message.collection}`);
			return;
		}

		const writeCollectionWorker = cluster.workers?.[writeCollectionWorkerId];
		const writeRecordWorker = Math.random() < 0.5
			? cluster.workers?.[workers.writeRecord.id]
			: cluster.workers?.[workers.writeRecord2.id];

		try {
			writeRecordWorker!.send(message);
			writeCollectionWorker!.send(message);
			if (
				workers.openSearch
				&& (message.collection === "app.bsky.feed.post"
					|| message.collection === "app.bsky.actor.profile")
			) {
				cluster.workers?.[workers.openSearch.id]?.send(message);
			}
			failedMessages.delete(message);
		} catch (e) {
			console.warn(`Failed to forward message to workers, retrying: ${e}`);
			failedMessages.add(message);
		}

		for (const msg of failedMessages) {
			forwardMessageToWorkers(worker, msg);
		}
	}

	function handleWorkerExit({ process: { pid } }: Worker, code: number, signal: string) {
		console.warn(`Worker ${pid} exited with code ${code} and signal ${signal}`);
		if (!pid) return;
		if (pid in workers.writeCollection) {
			spawnWriteCollectionWorker(workers.writeCollection[pid].index);
		} else if (pid === workers.writeRecord.pid) {
			cluster.workers?.[workers.writeRecord.id]?.kill();
			spawnWriteRecordWorker();
		} else if (pid === workers.writeRecord2.pid) {
			cluster.workers?.[workers.writeRecord2.id]?.kill();
			spawnWriteRecordWorker();
		} else if (pid in workers.repo) {
			spawnRepoWorker();
		} else if (pid === workers.openSearch?.pid) {
			spawnOpenSearchWorker();
		} else {
			console.error(`Unknown worker kind: ${pid}`);
		}
	}
}
