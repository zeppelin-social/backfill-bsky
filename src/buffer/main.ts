import { AppViewIndexer, type IndexerOptions } from "@futuristick/bsky-indexer";
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

class ToBufferIndexer extends AppViewIndexer {
	private stream: fs.WriteStream;

	constructor(filename: string, opts: Omit<IndexerOptions, "databaseOptions">) {
		super({ ...opts, minWorkers: 0, maxWorkers: 0, databaseOptions: { url: "" } });

		this.stream = fs.createWriteStream(filename);

		process.on("SIGINT", () => this.stream.close());
		process.on("SIGTERM", () => this.stream.close());
		process.on("exit", () => this.stream.close());
	}

	protected override initializeWorker() {}
	protected override checkScaling() {}

	protected override sendToWorker(chunk: Uint8Array): Promise<void> {
		const lengthBuffer = new Uint8Array(4);
		new DataView(lengthBuffer.buffer).setUint32(0, chunk.length, true);
		this.stream.write(lengthBuffer);
		this.stream.write(chunk);
		return Promise.resolve();
	}
}

function main() {
	const filename = "relay.buffer";

	const indexer = new ToBufferIndexer(filename, {
		service: process.env.BUFFER_REPO_PROVIDER,
		unauthenticatedCommits: true,
		unauthenticatedHandles: true,
	});

	return indexer.start();
}

void main();
