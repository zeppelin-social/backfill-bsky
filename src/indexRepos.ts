import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";

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

	const sub = new bsky.RepoSubscription({
		service: process.env.BSKY_REPO_PROVIDER,
		db,
		idResolver,
	});

	const indexingSvc = sub.indexingSvc;

	for (const did of process.argv.slice(2)) {
		await indexingSvc.indexRepo(did);
	}
}

void main();
