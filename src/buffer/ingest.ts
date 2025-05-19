import { FirehoseSubscription, type FirehoseSubscriptionOptions } from "@futur/bsky-indexer";
import fs from "node:fs";
import type { Readable } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";

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

// whether to persist and restore position from file
let useFileState = false;
if (process.argv.join(" ").includes("--file-state")) {
	useFileState = true;
}

let maxPerSecond = 2500;
if (process.argv.join(" ").includes("--max-per-second")) {
	maxPerSecond = parseInt(process.argv[process.argv.indexOf("--max-per-second") + 1]);
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

					// If message is obviously broken (>10MB),
					if (totalLength > 10_000_000) {
						// skip forward byte by byte until we find what looks like a valid message start
						let validStart = 1;
						while (validStart < buffer.length - 4) {
							const nextLength = buffer.readUInt32LE(validStart);
							// basic sanity checks for a valid message:
							// 1. Length should be <10MB
							// 2. Total message should fit in remaining buffer
							// 3. First byte of CBOR should be valid
							if (
								nextLength < 10_000_000
								&& validStart + nextLength + 4 <= buffer.length
								&& (buffer[validStart + 4] & 0xE0) <= 0xA0
							) {
								console.warn(`Skipped ${validStart} bytes to next valid message`);
								buffer = buffer.subarray(validStart);
								this.position += validStart;
								break;
							}
							validStart++;
						}
						continue;
					}

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
			fs.writeFileSync("relay.buffer.pos", (this.reader.position + 1).toString());
			lastPosition = this.reader.position;
		}, 10_000);

		try {
			let messagesSinceTimeout = 0;
			for await (const chunk of this.reader.read()) {
				messagesSinceTimeout++;
				void this.onMessage({ data: chunk } as MessageEvent<ArrayBuffer>);
				if (messagesSinceTimeout >= (maxPerSecond / 10)) {
					messagesSinceTimeout = 0;
					await sleep(100);
				}
			}

			// Kill ingest after 10 seconds of inactivity
			const destroyTimeout = setTimeout(() => {
				console.log("Buffer ingest complete");
				void this.destroy();
			}, 10_000);
			const onProcessed = this.onProcessed;
			this.onProcessed = (res) => {
				onProcessed(res);
				destroyTimeout.refresh();
			};

			// Kill all workers after 300 seconds regardless of activity
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
	let startPosition = parseInt(process.argv[2] || "0");
	if (useFileState) startPosition = parseInt(fs.readFileSync("relay.buffer.pos", "utf-8").trim());
	if (isNaN(startPosition)) startPosition = 0;

	const reader = new BufferReader("relay.buffer", startPosition);

	const indexer = new FromBufferSubscription(reader, {
		service: "",
		// Keep low to avoid deadlock
		minWorkers: 4,
		maxWorkers: 8,
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
