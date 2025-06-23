import { iterateAtpRepo } from "@atcute/car";
import { simpleFetchHandler, XRPC } from "@atcute/client";
import type { At } from "@atcute/client/lexicons";
import { parse as parseTID } from "@atcute/tid";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { IndexingService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";
import { CID } from "multiformats/cid";

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
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

const aggregatesUpdates: Array<Promise<void>> = [];

async function main() {
	const args = process.argv.slice(2);
	let spider = false;
	{
		const spiderIndex = args.indexOf("--spider");
		if (spiderIndex !== -1) {
			args.splice(spiderIndex, 1);
			spider = true;
		}
	}

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 100,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	await Promise.all(args.map(async (did) => {
		if (!did.startsWith("did:")) {
			did = await idResolver.handle.resolve(did).then((r) => {
				if (!r) throw new Error(`Invalid DID/handle: ${did}`);
				return r;
			});
		}
		return indexRepo(did, indexingSvc, spider);
	}));

	await Promise.all(aggregatesUpdates).catch((error) =>
		console.error("failed to update aggregates", error)
	);
}

async function indexRepo(did: string, indexingSvc: IndexingService, spider = false) {
	const commitData = await getRepoRecords(did, indexingSvc);
	if (Object.keys(commitData).length === 0) return;

	const records = Object.values(commitData).flat();
	const collections = new Map(Object.entries(commitData));

	const promises = [
		indexingSvc.bulkIndexToRecordTable(records),
		indexingSvc.bulkIndexToCollectionSpecificTables(collections),
		indexingSvc.indexHandle(did, new Date().toISOString()),
	];

	const follows = collections.get("app.bsky.graph.follow") || [];
	if (spider && follows.length) {
		console.log(`spidering ${follows.length} follows for ${did}`);
		promises.push(
			...follows.map(({ obj }) => indexRepo((obj as any).subject, indexingSvc, false)),
		);
		aggregatesUpdates.push(
			(indexingSvc.records.follow as any).params.updateAggregatesBulk?.(
				indexingSvc.db.db,
				follows.map(({ obj, uri, cid, timestamp }) => ({
					creator: did,
					subjectDid: (obj as any).subject,
					uri,
					cid,
					createdAt: timestamp,
					indexedAt: timestamp,
					sortAt: timestamp,
				})),
			),
		);
	}

	await Promise.all(promises).catch((error) => {
		console.error(`failed to index repo for ${did}`, error);
	});
}

async function getRepoRecords(did: string, indexingSvc: IndexingService) {
	try {
		const { idResolver } = indexingSvc;
		const { pds } = await idResolver.did.resolveAtprotoData(did);
		const agent = new XRPC({ handler: simpleFetchHandler({ service: pds }) });
		const { data: repo } = await agent.get(`com.atproto.sync.getRepo`, {
			params: { did: did as At.DID },
		});

		const records: Record<
			string,
			Array<{ uri: AtUri; cid: CID; timestamp: string; obj: unknown }>
		> = {};
		const now = Date.now();
		for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
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

			const data = {
				uri: AtUri.make(did, collection, rkey),
				cid: CID.parse(cid.$link),
				timestamp: new Date(indexedAt).toISOString(),
				obj: record,
			};

			(records[collection] ??= []).push(data);
		}
		return records;
	} catch (error) {
		console.warn(`failed to index repo for ${did}`, error);
		return {};
	}
}

void main();
