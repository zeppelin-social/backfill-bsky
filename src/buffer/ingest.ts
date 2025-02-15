import { FirehoseSubscription, type FirehoseSubscriptionOptions } from "@futuristick/bsky-indexer";
import fs from "node:fs";
import type { Readable } from "node:stream";

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

	constructor(filename: string) {
		this.stream = fs.createReadStream(filename);
	}

	async *read(): AsyncGenerator<Uint8Array> {
		let buffer = Buffer.alloc(0);

		try {
			for await (const chunk of this.stream) {
				buffer = Buffer.concat([buffer, chunk as Buffer]);

				while (buffer.length >= 4) {
					// Read message length
					const messageLength = buffer.readUInt32LE(0);
					const totalLength = messageLength + 4;

					// Check if we have the complete message
					if (buffer.length >= totalLength) {
						// Extract the message
						const message = new Uint8Array(buffer.subarray(4, totalLength));

						// Remove processed data from buffer
						buffer = buffer.subarray(totalLength);

						// Yield the message
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
		try {
			for await (const chunk of this.reader.read()) {
				// @ts-expect-error
				const worker = await this.getNextWorker();
				worker.postMessage({ type: "chunk", data: chunk });
			}
		} catch (err) {
			console.error(err);
		}
	}
}

async function main() {
	const reader = new BufferReader("relay.buffer");

	const indexer = new FromBufferSubscription(reader, {
		service: "",
		minWorkers: 10,
		maxWorkers: 10,
		idResolverOptions: { plcUrl: process.env.BSKY_DID_PLC_URL },
		dbOptions: {
			url: process.env.BSKY_DB_POSTGRES_URL,
			schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
			poolSize: 400,
		},
		onError: (err) => console.error(...(err.cause ? [err.message, err.cause] : [err])),
	});

	return indexer.start();
}

void main();
