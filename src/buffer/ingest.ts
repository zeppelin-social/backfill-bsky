import { AppViewIndexer } from "@futuristick/bsky-indexer";
import fs from "node:fs";
import type { Readable } from "node:stream";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_DB_POSTGRES_URL: string;
			BSKY_DB_POSTGRES_SCHEMA: string;
			BSKY_REPO_PROVIDER: string;
			BSKY_DID_PLC_URL: string;
		}
	}
}

for (
	const envVar of [
		"BSKY_DB_POSTGRES_URL",
		"BSKY_DB_POSTGRES_SCHEMA",
		"BSKY_REPO_PROVIDER",
		"BSKY_DID_PLC_URL",
	]
) {
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

async function main() {
	const reader = new BufferReader("relay.buffer");

	const indexer = new AppViewIndexer({
		service: process.env.BSKY_REPO_PROVIDER,
		unauthenticatedCommits: true,
		unauthenticatedHandles: true,
		maxWorkers: 25,
		identityResolverOptions: { plcUrl: process.env.BSKY_DID_PLC_URL },
		databaseOptions: {
			url: process.env.BSKY_DB_POSTGRES_URL,
			schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
			poolSize: 500,
		},
		sub: reader.read(),
	});

	return indexer.start();
}

void main();
