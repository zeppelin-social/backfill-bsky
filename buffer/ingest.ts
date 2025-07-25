import { readCar as iterateCar } from "@atcute/car";
import { MemoryCache } from "@atproto/identity";
import { lexToJson } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { type Event, jsonToLex, parseCid } from "@futur/bsky-indexer";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky-backfill";
import console from "node:console";
import { createReadStream, readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import readline from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";
import PQueue from "p-queue";
import { IdResolver, IndexingService } from "../backfill/indexingService.ts";
import type { ToInsertCommit } from "../backfill/workers/writeCollection.ts";

const BATCH_SIZE = 50_000;
const LOG_INTERVAL_MS = 30_000;

const useFileState = process.argv.includes("--file-state");

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_DB_POSTGRES_URL: string;
			BSKY_DB_POSTGRES_SCHEMA: string;
			BSKY_DID_PLC_URL: string;
		}
	}
}

for (const envVar of ["BSKY_DB_POSTGRES_URL", "BSKY_DB_POSTGRES_SCHEMA", "BSKY_DID_PLC_URL"]) {
	if (!process.env[envVar]) throw new Error(`Missing required env var ${envVar}`);
}

type CollectionMap = Map<string, ToInsertCommit[]>;

async function main() {
	const file = "relay-buffer.jsonl";

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 150,
	});
	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});
	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	let startLine = 0;
	if (useFileState) {
		try {
			startLine = parseInt(readFileSync("relay-buffer.pos", "utf-8").trim());
		} catch {
			// Start at 0
		}
	}
	if (Number.isNaN(startLine)) startLine = 0;
	console.log(`Starting buffer ingest at line ${startLine}`);

	const rl = readline.createInterface({
		input: createReadStream(file, { encoding: "utf8" }),
		crlfDelay: Infinity,
	});

	let lineNo = 0;
	let sinceLastLog = Date.now();

	let recordBuffer: ToInsertCommit[] = [];
	let collectionBuffer: CollectionMap = new Map();
	const otherEvts = new PQueue({ concurrency: 20 });
	otherEvts.on("error", console.error);

	const onExit = () => {
		console.log("Received SIGINT, trying to gracefully exit");
		Promise.race([flush(indexingSvc, lineNo, true), sleep(10_000)]).then(() => process.exit(0));
	};
	process.on("SIGINT", onExit);
	process.on("SIGTERM", onExit);
	process.on("SIGQUIT", onExit);

	for await (const line of rl) {
		lineNo++;

		if (lineNo <= startLine) continue;
		if (!line) continue;

		let evt;
		try {
			evt = jsonToLex(JSON.parse(line)) as Event;
		} catch {
			console.warn(`Failed to parse JSON at line ${lineNo}`);
			continue;
		}

		try {
			processEvent(evt);
		} catch (err) {
			console.warn(`Failed to process event at line ${lineNo}`, err);
		}

		if (recordBuffer.length >= BATCH_SIZE) {
			await flush(indexingSvc, lineNo);
			collectionBuffer = new Map();
			recordBuffer = [];
		}

		if (Date.now() - sinceLastLog > LOG_INTERVAL_MS) {
			console.log(
				`Processed ${lineNo.toLocaleString()} lines `
					+ `(${recordBuffer.length.toLocaleString()} records buffered)`,
			);
			sinceLastLog = Date.now();
		}
	}

	await flush(indexingSvc, lineNo, true);
	console.log("Ingestion complete!");
	process.exit(0);

	async function flush(idxSvc: IndexingService, pos: number, final = false) {
		if (recordBuffer.length === 0) return;

		const t0 = Date.now();
		const collectionCount = [...collectionBuffer.values()].reduce(
			(acc, arr) => acc + arr.length,
			0,
		);

		try {
			await Promise.all([
				idxSvc.bulkIndexToRecordTable(recordBuffer),
				idxSvc.bulkIndexToCollectionSpecificTables(collectionBuffer, { validate: false }),
				otherEvts.onSizeLessThan(100), // Just want to prevent it from getting too big
			]);
			const dt = ((Date.now() - t0) / 1000).toFixed(1);
			console.log(
				`Flushed ${recordBuffer.length.toLocaleString()} records (${collectionCount.toLocaleString()} collection rows) in ${dt}s`,
			);
			writeFileSync("relay-buffer.pos", `${pos}\n`);
		} catch (err) {
			console.error("Error flushing batch – writing to disk", err);
			writeFileSync(
				`failed-batch-${Date.now()}.jsonl`,
				recordBuffer.map((r) => JSON.stringify({ uri: r.uri, obj: lexToJson(r.obj) })).join(
					"\n",
				) + "\n",
			);
			if (final) throw err;
		}
	}

	function processEvent(evt: Event) {
		if (evt.$type === "com.atproto.sync.subscribeRepos#identity") {
			void otherEvts.add(() => indexingSvc.indexHandle(evt.did, evt.time, true));
			return;
		} else if (evt.$type === "com.atproto.sync.subscribeRepos#account") {
			if (evt.active === false && evt.status === "deleted") {
				void otherEvts.add(() => indexingSvc.deleteActor(evt.did));
			} else {
				void otherEvts.add(() =>
					indexingSvc.updateActorStatus(evt.did, evt.active, evt.status)
				);
			}
			return;
		} else if (evt.$type === "com.atproto.sync.subscribeRepos#sync") {
			const cid = parseCid(iterateCar(evt.blocks).header.data.roots[0]);
			void otherEvts.add(() =>
				Promise.all([
					indexingSvc.setCommitLastSeen(evt.did, cid, evt.rev),
					indexingSvc.indexHandle(evt.did, evt.time),
				])
			);
			return;
		} else if (evt.$type !== "com.atproto.sync.subscribeRepos#commit") return;

		if (!evt.ops?.length) return;

		for (const op of evt.ops) {
			const uri = AtUri.make(evt.did, ...op.path.split("/"));
			if (op.action === "delete") {
				void otherEvts.add(() => indexingSvc.deleteRecord(uri));
			} else if (op.action === "update") {
				void otherEvts.add(() =>
					indexingSvc.indexRecord(
						uri,
						parseCid(op.cid),
						op.record,
						WriteOpAction.Update,
						evt.time,
					)
				);
			} else {
				if (!collectionBuffer.has(uri.collection)) collectionBuffer.set(uri.collection, []);
				collectionBuffer.get(uri.collection)!.push({
					uri,
					cid: parseCid(op.cid),
					timestamp: evt.time,
					obj: op.record,
				});
			}
		}
	}
}

main().catch((err) => {
	console.error("Ingest error", err);
	process.exit(1);
});
