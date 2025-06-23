import { simpleFetchHandler, XRPC } from "@atcute/client";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { AtUri } from "@atproto/syntax";
import { BackgroundQueue, Database } from "@futuristick/atproto-bsky";
import { IndexingService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";
import { CID } from "multiformats/cid";
import { is } from "../backfill/util/lexicons";
import { jsonToLex } from "../backfill/workers/writeCollection";

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
	const args = process.argv.slice(2);

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

	// I don't like using bsky.social but it's convenient
	const xrpc = new XRPC({ handler: simpleFetchHandler({ service: "https://bsky.social" }) });

	const profiles: Array<{ uri: AtUri; cid: CID; obj: unknown; timestamp: string }> = [];

	await Promise.all(args.map(async (did) => {
		if (!did.startsWith("did:")) {
			did = await idResolver.handle.resolve(did).then((r) => {
				if (!r) throw new Error(`Invalid DID/handle: ${did}`);
				return r;
			});
		}

		const profile = await xrpc.get("com.atproto.repo.getRecord", {
			params: { repo: did, collection: "app.bsky.actor.profile", rkey: "self" },
		});
		if (!is("app.bsky.actor.profile", profile.data.value)) return;
		profiles.push({
			uri: new AtUri(profile.data.uri),
			cid: CID.parse(profile.data.cid!),
			obj: jsonToLex(profile.data.value as Record<string, unknown>),
			timestamp: profile.data.value.createdAt ?? new Date().toISOString(),
		});
	}));

	await indexingSvc.bulkIndexToCollectionSpecificTables(
		new Map([["app.bsky.actor.profile", profiles]]),
		{ validate: false },
	);
}

void main();
