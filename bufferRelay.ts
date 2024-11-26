import { IdResolver } from "@atproto/identity";
import { Firehose } from "@atproto/sync";
import * as fs from "node:fs";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BGS_URL: string;
		}
	}
}

for (const envVar of ["BGS_URL"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

function main() {
	const ws = fs.createWriteStream("relay-buffer.jsonl");

	const idResolver = new IdResolver();
	const firehose = new Firehose({
		idResolver,
		service: process.env.BGS_URL,
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
