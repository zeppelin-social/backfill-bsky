import {
	FirehoseSubscription,
	type FirehoseSubscriptionOptions,
	type WorkerMessage,
} from "@futuristick/bsky-indexer";
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

class FakeWorker {
	constructor(private stream: fs.WriteStream) {}

	postMessage(message: WorkerMessage) {
		if (message.type === "chunk") {
			const { data: chunk } = message;
			const lengthBuffer = new Uint8Array(4);
			new DataView(lengthBuffer.buffer).setUint32(0, chunk.length, true);
			this.stream.write(lengthBuffer);
			this.stream.write(chunk);
		}
	}
}

// @ts-expect-error
class ToBufferSubscription extends FirehoseSubscription {
	private stream: fs.WriteStream;

	constructor(filename: string, opts: Omit<FirehoseSubscriptionOptions, "dbOptions">) {
		super({ ...opts, minWorkers: 0, maxWorkers: 0, dbOptions: { url: "" } });

		this.stream = fs.createWriteStream(filename);
		// @ts-expect-error
		this.workers = [new FakeWorker(this.stream)];

		process.on("exit", () => this.stream.close());
	}

	override checkScaling() {}
}

function main() {
	const filename = "relay.buffer";

	const sub = new ToBufferSubscription(filename, { service: process.env.BUFFER_REPO_PROVIDER });

	return sub.start();
}

void main();
