import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { BlobRef } from "@atproto/lexicon";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import { CID } from "multiformats/cid";
import * as shm from "shm-typed-array";

export type CommitData = { uri: string; cid: string; timestamp: string; obj: unknown };

export type CommitMessage = { type: "commit"; collection: string; data: CommitData };

export async function repoWorker() {
	const queue = new Queue<{ did: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
	});

	const redis = createClient();
	await redis.connect();

	queue.process(5, async (job) => {
		if (!process?.send) throw new Error("Not a worker process");

		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		const repo = shm.get(did, "Uint8Array");
		if (!repo?.byteLength) {
			console.warn(`Did not get repo for ${did}`);
			return;
		}

		try {
			console.time(`Processing repo: ${did}`);

			const now = Date.now();
			for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
				const uri = `at://${did}/${collection}/${rkey}`;

				// This should be the date the AppView saw the record, but since we don't want the "archived post" label
				// to show for every post in social-app, we'll try our best to figure out when the record was actually created.
				// So first we try createdAt then parse the rkey; if either of those is in the future, we'll use now.
				let indexedAt: number =
					(!!record && typeof record === "object" && "createdAt" in record
						&& typeof record.createdAt === "string"
						&& new Date(record.createdAt).getTime()) || 0;
				if (!indexedAt || isNaN(indexedAt)) {
					try {
						indexedAt = parseTID(rkey).timestamp;
					} catch {
						indexedAt = now;
					}
				}
				if (indexedAt > now) indexedAt = now;

				// The appview IndexingService does lex validation on the record, which only accepts blob refs in the
				// form of a BlobRef instance, so we need to do this expensive iteration over every single record
				convertBlobRefs(record);

				const data = {
					uri,
					cid: cid.$link,
					timestamp: new Date(indexedAt).toISOString(),
					obj: record,
				};
				process.send({ type: "commit", collection, data } satisfies CommitMessage);
			}
			await redis.sAdd("backfill:seen", did);
			console.timeEnd(`Processing repo: ${did}`);
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
		} finally {
			shm.destroy(did);
		}
	});

	queue.on("error", (err) => {
		console.error("Queue error:", err);
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
	});
}

function convertBlobRefs(obj: unknown): unknown {
	if (Array.isArray(obj)) {
		obj.forEach((item) => convertBlobRefs(item));
		return obj;
	} else {
		assertObject(obj);

		// weird-ish formulation but faster than for-in or Object.entries
		const keys = Object.keys(obj);
		let i = keys.length;
		while (i--) {
			const key = keys[i];
			const value = obj[key];
			if (typeof value === "object" && value !== null) {
				if (value.$type === "blob") {
					try {
						const cidLink = CID.parse(value.ref.$link);
						obj[key] = new BlobRef(cidLink, value.mimeType, value.size);
					} catch {
						console.warn(
							`Failed to parse CID ${value.ref.$link}\nRecord: ${
								JSON.stringify(obj)
							}`,
						);
						return obj;
					}
				} else {
					convertBlobRefs(value);
				}
			}
		}
	}

	return obj;
}

const assertObject: (obj: unknown) => asserts obj is Record<string, any> = (obj) => {
	if (typeof obj !== "object" || obj === null) {
		throw new Error("Expected object");
	}
};
