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

class BufferReader {
	private stream: Readable;
	public bufferSize: number;
	public position = 0;

	constructor(filename: string) {
		this.stream = fs.createReadStream(filename);
		this.bufferSize = fs.statSync(filename).size;
	}

	async *read(): AsyncGenerator<Uint8Array> {
		let buffer = Buffer.alloc(0);

		try {
			for await (const chunk of this.stream) {
				buffer = Buffer.concat([buffer, chunk as Buffer]);

				while (buffer.length >= 4) {
					const messageLength = buffer.readUInt32LE(0);
					const totalLength = messageLength + 4;

					// Check if we have the complete message
					if (buffer.length >= totalLength) {
						const message = new Uint8Array(buffer.subarray(4, totalLength));

						// Remove processed data from buffer
						buffer = buffer.subarray(totalLength);
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
		setInterval(() => {
			const progress = this.reader.position / this.reader.bufferSize * 100;
			console.log(`Buffer progress: ${progress.toFixed(2)}%`);
		}, 10_000);

		try {
			for await (const chunk of this.reader.read()) {
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
}

async function main() {
	const reader = new BufferReader("relay.buffer");

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

	return indexer.start();
}

void main();
