import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import { CID } from "multiformats/cid";
import fs from "node:fs";
import readline from "node:readline/promises";
import type { BackfillLine } from "./backfillCommits.js";

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

	const indexer = sub.indexingSvc;

	const rl = readline.createInterface({ input: fs.createReadStream("backfill-sorted.jsonl") });

	for await (const lineStr of rl) {
		let line: BackfillLine;
		try {
			line = JSON.parse(lineStr);
			if (
				line.action !== "create" || !line.cid || !line.record || !line.timestamp
				|| !line.uri
			) continue;
		} catch (err) {
			console.error(err);
			continue;
		}

		await indexer.indexRecord(
			new AtUri(line.uri),
			CID.parse(line.cid),
			line.record,
			WriteOpAction.Create,
			new Date(line.timestamp).toISOString(),
		);
	}
}

void main();
