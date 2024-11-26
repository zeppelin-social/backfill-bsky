import * as bsky from "@atproto/bsky";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { WriteOpAction } from "@atproto/repo";
import type { Event } from "@atproto/sync";
import fs from "node:fs";
import readline from "node:readline/promises";

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

	const rl = readline.createInterface({ input: fs.createReadStream("relay-buffer.jsonl") });

	for await (const line of rl) {
		let evt: Event;
		try {
			evt = JSON.parse(line);
		} catch (err) {
			console.error(err);
			continue;
		}

		// https://github.com/appview-wg-bsky/atproto/blob/bbab23a6b2d3ab44c0cdc83fef0616baa39c4e04/packages/bsky/src/data-plane/server/subscription.ts#L74C7-L100C8
		if (evt.event === "identity") {
			await indexingSvc.indexHandle(evt.did, evt.time, true);
		} else if (evt.event === "account") {
			if (evt.active === false && evt.status === "deleted") {
				await indexingSvc.deleteActor(evt.did);
			} else {
				await indexingSvc.updateActorStatus(evt.did, evt.active, evt.status);
			}
		} else {
			const indexFn = evt.event === "delete"
				? indexingSvc.deleteRecord(evt.uri)
				: indexingSvc.indexRecord(
					evt.uri,
					evt.cid,
					evt.record,
					evt.event === "create" ? WriteOpAction.Create : WriteOpAction.Update,
					evt.time,
				);
			await Promise.all([
				indexFn,
				indexingSvc.setCommitLastSeen(evt.did, evt.commit, evt.rev),
				indexingSvc.indexHandle(evt.did, evt.time),
			]);
		}
	}
}

void main();
