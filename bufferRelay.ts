import { IdResolver } from "@atproto/identity";
import { Firehose } from "@atproto/sync";
import * as fs from "node:fs";

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
	const firehose = new Firehose({
		idResolver,
		service: process.env.BSKY_REPO_PROVIDER,
		handleEvent: async (event) => {
			ws.write(JSON.stringify(event) + "\n");
		},
		onError: (err) => {
			console.error(`relay error: ${err}`);
		},
	});

	firehose.start();

	process.on("SIGINT", () => ws.close());
	process.on("SIGTERM", () => ws.close());
	process.on("exit", () => ws.close());
}

main();
