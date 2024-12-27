import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
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

	queue.process(30, async (job) => {
		if (!process?.send) throw new Error("Not a worker process");

		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		let repo;
		try {
			repo = shm.get(did, "Uint8Array");
			if (!repo?.byteLength) throw new Error("Got empty repo for " + did);
		} catch (err) {
			console.warn("Error while getting repo bytes for " + did, err);
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
		process.exit(1);
	});

	queue.on("failed", (job, err) => {
		console.error(`Job failed for ${job.data.did}:`, err);
		process.exit(1);
	});
}
