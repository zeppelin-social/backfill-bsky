import { IdResolver } from "@atproto/identity";
import {
	Firehose,
	MemoryRunner,
} from "@atproto/sync";
import * as fs from "node:fs";
import { serializeEvent } from './serialize.js'

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
		}
	}
}

for (const envVar of ["BSKY_REPO_PROVIDER"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

function main() {
	const ws = fs.createWriteStream("relay-buffer.jsonl");

	const idResolver = new IdResolver();
	const runner = new MemoryRunner({ startCursor: 0 });
	const firehose = new Firehose({
		idResolver,
		service: process.env.BSKY_REPO_PROVIDER,
		runner,
		unauthenticatedCommits: true,
		unauthenticatedHandles: true,
		handleEvent: async (event) => {
			const ser = serializeEvent(event);
			ws.write(JSON.stringify(ser) + "\n");
		},
		onError: (err) => {
			console.error(err);
		},
	});

	firehose.start();

	process.on("SIGINT", () => ws.close());
	process.on("SIGTERM", () => ws.close());
	process.on("exit", () => ws.close());
}

main();
