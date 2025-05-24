import {
	FirehoseSubscription,
	FirehoseSubscriptionError,
	type FirehoseSubscriptionOptions,
} from "@futur/bsky-indexer";
import { TextLineStream } from "@std/streams/text-line-stream";
import { Blob, Buffer } from "node:buffer";
import console from "node:console";
import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { setInterval, setTimeout } from "node:timers";
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

// maximum number of messages to read per second
let maxPerSecond = 2500;
if (process.argv.join(" ").includes("--max-per-second")) {
	maxPerSecond = parseInt(process.argv[process.argv.indexOf("--max-per-second") + 1]);
}

Buffer.poolSize = 0;

class FromBufferSubscription extends FirehoseSubscription {
	position = 0;

	constructor(
		private filename: string,
		private startPosition: number,
		options: FirehoseSubscriptionOptions,
	) {
		const { dbOptions, idResolverOptions } = options;
		const workerPath = new URL("./ingestWorker.ts", import.meta.url);
		const workerBlob = new Blob([
			readFileSync(workerPath),
			`\nexport default new IngestWorker(${
				JSON.stringify({ dbOptions, idResolverOptions })
			});`,
		], { type: "application/typescript" });
		super(options, workerBlob);
	}

	override async start() {
		try {
		  const lineCount = await this.estimateLineCount(this.filename);
		  console.log(`estimated ${lineCount} lines in ${this.filename}`);

				let messagesSinceTimeout = 0;

			setInterval(() => {
				if (this.position > this.startPosition) {
					writeFileSync("relay-buffer.pos", `${this.position}`);
					console.log(`read ${this.position}/~${lineCount} lines`);
				}
			}, 30_000);

			using fh = await Deno.open(this.filename);
			for await (
				const line of fh.readable.pipeThrough(new TextDecoderStream()).pipeThrough(
					new TextLineStream(),
				)
			) {
				messagesSinceTimeout++;
				this.position++;
				if (this.position < this.startPosition) continue;
				if (messagesSinceTimeout >= maxPerSecond / 10) {
					messagesSinceTimeout = 0;
					await sleep(1000 / 10);
				}

				void this.onMessage(line);
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

	// @ts-expect-error — onMessage expects a MessageEvent<ArrayBuffer>
	override onMessage = async (line: string): Promise<void> => {
		try {
			// @ts-expect-error — should make pool type generic
			const res = await this.pool.execute({ line });
			this.onProcessed(res);
		} catch (e) {
			this.opts.onError?.(new FirehoseSubscriptionError(e));
		}
	};

	private async estimateLineCount(filepath: string): Promise<number> {
			const { size: totalSize } = await Deno.stat(filepath);
			using file = await Deno.open(filepath, { read: true });

			const decoder = new TextDecoder();
			const buffer = new Uint8Array(32 * 1024);
			let bytesRead = 0;
			let lineCount = 0;
			let partialLine = "";
			let sampleSize = 0;

			while (lineCount < 1000) {
				const n = await file.read(buffer);

				if (n === null) {
					if (partialLine.length > 0) {
						lineCount++;
						sampleSize += new TextEncoder().encode(partialLine).length;
					}
					break;
				}

				bytesRead += n;
				const chunk = decoder.decode(buffer.subarray(0, n), { stream: true });
				const text = partialLine + chunk;
				const lines = text.split("\n");

				partialLine = lines.pop() || "";

				for (const line of lines) {
					if (lineCount < 1000) {
						lineCount++;
						sampleSize += new TextEncoder().encode(line).length + 1;
					} else {
						break;
					}
				}
			}

			if (bytesRead >= totalSize && lineCount < 1000) {
				return lineCount;
			}

			if (lineCount === 0) {
				return 0;
			}

			const avgBytesPerLine = sampleSize / lineCount;
			return Math.round(totalSize / avgBytesPerLine);
	}
}

async function main() {
	let startPosition = parseInt(process.argv[2] || "0");
	if (useFileState) {
		startPosition = parseInt(readFileSync("relay-buffer.pos", "utf-8").trim());
	}
	if (isNaN(startPosition)) startPosition = 0;

	const file = "relay-buffer.jsonl";

	const indexer = new FromBufferSubscription(file, startPosition, {
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
		console.log(`Exiting with position ${indexer.position}`);
		return indexer.destroy();
	};
	process.on("SIGINT", onExit);
	process.on("SIGPIPE", onExit);
	process.on("SIGTERM", onExit);
	process.on("beforeExit", onExit);

	return indexer.start();
}

void main();
