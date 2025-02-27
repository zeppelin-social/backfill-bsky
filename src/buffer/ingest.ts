import { FirehoseSubscription, type FirehoseSubscriptionOptions } from "@futuristick/bsky-indexer";
import fs from "node:fs";
import type { Readable } from "node:stream";
import type { Worker } from "node:worker_threads";

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

Buffer.poolSize = 0;

class BufferReader {
	private stream: Readable;
	public bufferSize: number;

	constructor(filename: string, public position = 0) {
		this.stream = fs.createReadStream(filename, { start: Math.max(0, position - 1) });
		this.bufferSize = fs.statSync(filename).size;
	}

	async *read(): AsyncGenerator<Buffer> {
		let buffer = Buffer.alloc(0);

		try {
			let chunk: Buffer;
			for await (chunk of this.stream) {
				buffer = Buffer.concat([buffer, chunk]);

				while (buffer.length >= 4) {
					const messageLength = buffer.readUInt32LE(0);
					const totalLength = messageLength + 4;

					// Check if we have the complete message
					if (buffer.length >= totalLength) {
						const message = Buffer.from(buffer.subarray(4, totalLength));

						// Remove processed data from buffer
						buffer = Buffer.from(buffer.subarray(totalLength));
						this.position += totalLength;

						yield message;
					} else {
						// Wait for more data
						break;
					}
				}
			}

			if (buffer.length > 0) {
				console.warn("Incomplete message at end of file");
			}
		} finally {
			this.stream.destroy();
		}
	}
}

class FromBufferSubscription extends FirehoseSubscription {
	constructor(private readonly reader: BufferReader, options: FirehoseSubscriptionOptions) {
		super(options);
	}

	override async start() {
		let lastPosition = 0;
		setInterval(() => {
			const progress = this.reader.position / this.reader.bufferSize * 100;
			const diffKb = Math.abs(this.reader.position - lastPosition) / 1000;
			console.log(
				`Buffer progress: ${progress.toFixed(2)}% | ${
					(diffKb / 10).toFixed(2)
				}kb/s | pos: ${this.reader.position}`,
			);
			lastPosition = this.reader.position;
		}, 10_000);

		try {
			for await (const chunk of this.reader.read()) {
				// @ts-expect-error
				if (this.destroyed) break;
				// @ts-expect-error
				const worker = await this.getNextWorker();
				worker.postMessage({ type: "chunk", data: chunk });
			}

			// @ts-expect-error
			const workers: Worker[] = this.workers;
			let remainingWorkers = workers.length;

			workers.forEach((worker: Worker, i: number) => {
				// Kill each worker after 20 seconds without a message
				const timeout = setTimeout(() => {
					console.warn(`Worker ${i} timed out`);
					worker.terminate();

					remainingWorkers--;
					if (remainingWorkers === 0) {
						process.exit();
					}
				}, 20_000);

				worker.on("message", () => {
					timeout.refresh();
				});
			});

			// Kill all workers after 300 seconds
			setTimeout(() => {
				console.warn("All workers timed out");
				process.exit();
			}, 300_000);
		} catch (err) {
			console.error(err);
		}
	}

	override async destroy() {
		// @ts-expect-error
		this.destroyed = true;
	}
}

async function main() {
	const startPosition = parseInt(process.argv[2] || "0");
	if (isNaN(startPosition)) throw new Error("Invalid start position");

	const reader = new BufferReader("relay.buffer", startPosition);

	const indexer = new FromBufferSubscription(reader, {
		service: "",
		// Keep low to avoid deadlock
		minWorkers: 4,
		maxWorkers: 4,
		idResolverOptions: { plcUrl: process.env.BSKY_DID_PLC_URL },
		dbOptions: {
			url: process.env.BSKY_DB_POSTGRES_URL,
			schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
			poolSize: 500,
		},
		onError: (err) => console.error(...(err.cause ? [err.message, err.cause] : [err])),
	});

	const onExit = () => {
		console.log(`Exiting with position ${reader.position}`);
		return indexer.destroy();
	};
	process.on("SIGINT", onExit);
	process.on("SIGPIPE", onExit);
	process.on("SIGTERM", onExit);
	process.on("beforeExit", onExit);

	return indexer.start();
}

void main();
