import { createClient } from "@redis/client";
import * as bsky from "@zeppelin-social/bsky-backfill";
import Queue from "bee-queue";
import CacheableLookup from "cacheable-lookup";
import { LRUCache } from "lru-cache";
import cluster, { type Worker } from "node:cluster";
import fs from "node:fs/promises";
import * as os from "node:os";
import { setTimeout as sleep } from "node:timers/promises";
import { Agent, RetryAgent, setGlobalDispatcher } from "undici";
import { fetchAllDids } from "./util/fetch.js";
import { openSearchWorker } from "./workers/opensearch.js";
import { type CommitMessage, repoWorker } from "./workers/repo.js";
import { writeCollectionWorker, writeWorkerAllocations } from "./workers/writeCollection.js";
import { writeRecordWorker } from "./workers/writeRecord.js";

export type FromWorkerMessage = CommitMessage | { type: "shutdownComplete" } | {
	type: "count";
	fetched: number;
	parsed: number;
};

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
	const args = process.argv.slice(2);
	const indexOfInspect = args.findIndex((a) => a.startsWith("--inspect"));
	cluster.setupPrimary({ args: indexOfInspect > -1 ? args.toSpliced(indexOfInspect, 1) : args });

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
		const worker = cluster.fork({ WORKER_KIND: "repo" });
		if (!worker.process?.pid) throw new Error("Worker process not found");
		workers.repo[worker.process.pid] = { kind: "repo" };
		worker.on("error", (err) => {
			console.error(`Repo worker error: ${err}`);
			worker.kill();
			cluster.fork({ WORKER_KIND: "repo" });
		});
	};

	const numCPUs = os.availableParallelism();
	const repoWorkerCount = Math.max(16, Math.min(numCPUs * 2, 64));
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

	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 5,
	});

	await Promise.all(
		Object.entries(DB_SETTINGS).map(([setting, value]) =>
			db.pool.query(`ALTER SYSTEM SET ${setting} = ${value}`)
		),
	);

	const redis = createClient();
	await redis.connect();

	const queue = new Queue<{ did: string; pds: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
		isWorker: false,
	});

	let isShuttingDown = false;

	cluster.on("exit", handleWorkerExit);

	cluster.on("message", handleFromWorkerMessage);

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

	process.on("SIGINT", async () => {
		console.log("\nReceived SIGINT. Starting graceful shutdown...");
		isShuttingDown = true;

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

		if (failedMessages?.size) {
			await fs.writeFile(
				"./failed-worker-messages.jsonl",
				JSON.stringify([...failedMessages.keys()]),
			);
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

	let processedOverInterval = 0, fetchedOverInterval = 0;

	setInterval(async () => {
		const processed = processedOverInterval / 5, fetched = fetchedOverInterval / 5;
		processedOverInterval = 0;
		fetchedOverInterval = 0;

		console.log(
			`Processed repos: ${processed.toFixed(1)}/s | Fetched repos: ${fetched.toFixed(1)}/s`,
		);
	}, 5_000);

	async function main() {
		console.log(`Seen: ${await redis.sCard("backfill:seen")} DIDs`);

		for await (const [did, pds] of fetchAllDids()) {
			if (isShuttingDown) break;
			// dumb pds doesn't implement getRepo
			if (pds.includes("blueski.social")) continue;
			if (await redis.sIsMember("backfill:seen", did)) continue;
			await waitUntilQueueLessThan(queue, 5_000);
			void queue.createJob({ did, pds }).setId(did).save().catch((e) =>
				console.error(`Error queuing repo for ${did} `, e)
			);
		}
	}

	void main();

	const failedMessages = new LRUCache<CommitMessage, true>({ max: 100_000 });
	function handleFromWorkerMessage(worker: Worker, message: FromWorkerMessage) {
		if (message.type === "shutdownComplete") {
			if (!isShuttingDown) handleWorkerExit(worker, 0, "SIGINT");
			if (!worker.process.killed) worker.kill();
			return;
		}

		if (message.type === "count") {
			fetchedOverInterval += message.fetched;
			processedOverInterval += message.parsed;
			return;
		}

		if (message?.type !== "commit" || !message.collection || !message.commits) {
			throw new Error(`Received invalid worker message: ${JSON.stringify(message)}`);
		}

		forwardCommitsToWorkers(message);
	}

	function forwardCommitsToWorkers(message: CommitMessage) {
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
			failedMessages.set(message, true);
		}
	}

	setInterval(() => {
		const messages = [...failedMessages.keys()];
		failedMessages.clear();

		for (const msg of messages) {
			forwardCommitsToWorkers(msg);
		}
	}, 5000);

	function handleWorkerExit({ process: { pid } }: Worker, code: number, signal: string) {
		console.warn(`Worker ${pid} exited with code ${code} and signal ${signal}`);
		if (isShuttingDown) return;
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

	async function waitUntilQueueLessThan(queue: Queue, size: number) {
		do {
			const { waiting, active } = await queue.checkHealth();
			if (waiting + active < size) return;
			await sleep(500);
		} while (true);
	}
}
