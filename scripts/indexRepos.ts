import { iterateAtpRepo } from "@atcute/car";
import { simpleFetchHandler, XRPC } from "@atcute/client";
import type { At } from "@atcute/client/lexicons";
import { parse as parseTID } from "@atcute/tid";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { IndexingService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";

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

async function main() {
	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 10,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const indexingSvc = new IndexingService(db, idResolver, new BackgroundQueue(db));

	await Promise.all(
		process.argv.slice(2).map(async (did) => {
			if (!did.startsWith("did:")) {
				did = await idResolver.handle.resolve(did).then((r) => {
					if (!r) throw new Error(`Invalid DID/handle: ${did}`);
					return r;
				});
			}
			return indexRepo(did, indexingSvc);
		}),
	);
}

async function indexRepo(did: string, indexingSvc: IndexingService) {
	const { idResolver } = indexingSvc;
	const { pds } = await idResolver.did.resolveAtprotoData(did);
	const agent = new XRPC({ handler: simpleFetchHandler({ service: pds }) });
	const { data: repo } = await agent.get(`com.atproto.sync.getRepo`, {
		params: { did: did as At.DID },
	});

	const commitData: Record<
		string,
		Array<{ did: string; path: string; cid: string; timestamp: string; obj: unknown }>
	> = {};
	const now = Date.now();
	for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
		const path = `${collection}/${rkey}`;

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
			did,
			path,
			cid: cid.$link,
			timestamp: new Date(indexedAt).toISOString(),
			obj: record,
		};

		(commitData[collection] ??= []).push(data);
	}

	const records = Object.values(commitData).flat();
	const collections = new Map(Object.entries(commitData));
	await Promise.all([
		indexingSvc.bulkIndexToRecordTable(records),
		indexingSvc.bulkIndexToCollectionSpecificTables(collections),
	]);
}

void main();
