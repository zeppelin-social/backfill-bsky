import { IdResolver, MemoryCache } from "@atproto/identity";
import * as bsky from "@futuristick/atproto-bsky";

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
	const db = new bsky.Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: 10,
	});

	const idResolver = new IdResolver({
		plcUrl: process.env.BSKY_DID_PLC_URL,
		didCache: new MemoryCache(),
	});

	const { indexingSvc } = new bsky.RepoSubscription({ service: "", db, idResolver });

	for (let did of process.argv.slice(2)) {
		if (!did.startsWith("did:")) {
			did = await idResolver.handle.resolve(did).then((r) => {
				if (!r) throw new Error(`Invalid DID/handle: ${did}`);
				return r;
			});
		}
		await indexingSvc.indexHandle(did, new Date().toISOString());
		await indexingSvc.indexRepo(did);
	}
}

void main();
