import { FirehoseSubscription, type FirehoseSubscriptionOptions } from "@futur/bsky-indexer";
import * as fs from "node:fs";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BUFFER_REPO_PROVIDER: string;
		}
	}
}

for (const envVar of ["BUFFER_REPO_PROVIDER"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

class ToBufferSubscription extends FirehoseSubscription {
	private stream: fs.WriteStream;

	constructor(opts: Omit<FirehoseSubscriptionOptions, "dbOptions">, filename: string) {
		super({
			...opts,
			minWorkers: 0,
			maxWorkers: 1,
			dbOptions: { url: "" },
			statsFrequencyMs: 0,
		});

		void this.pool.destroy();
		this.stream = fs.createWriteStream(filename, { flags: "a" });
		process.on("exit", () => this.stream.close());
	}

	override onMessage = async ({ data }: MessageEvent<ArrayBuffer>) => {
		const chunk = new Uint8Array(data);
		this.stream.write(Buffer.from(chunk));
	};
}

function main() {
	const filename = "relay.buffer";

	const sub = new ToBufferSubscription({
		service: process.env.BUFFER_REPO_PROVIDER,
		cursor: 0,
		onError: (err) => console.error(...(err.cause ? [err.message, err.cause] : [err])),
	}, filename);

	return sub.start();
}

void main();
