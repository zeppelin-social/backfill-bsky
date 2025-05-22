import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { createClient } from "@redis/client";
import Queue from "bee-queue";
import { heapStats } from "bun:jsc";
import fs from "node:fs/promises";
import path from "node:path";

export type CommitData = { uri: string; cid: string; timestamp: string; obj: unknown };

export type CommitMessage = { type: "commit"; collection: string; commits: CommitData[] };

export async function repoWorker() {
	for (const envVar of ["REPOS_DIR"]) {
		if (!process.env[envVar]) {
			throw new Error(`Repo worker missing env var ${envVar}`);
		}
	}

	const redis = createClient();
	await redis.connect();

	const queue = new Queue<{ did: string }>("repo-processing", {
		removeOnSuccess: true,
		removeOnFailure: true,
	});

	let commitData: Record<string, CommitData[]> = {};

	queue.process(25, async (job) => {
		if (!process?.send) throw new Error("Not a worker process");

		const { did } = job.data;

		if (!did || typeof did !== "string") {
			console.warn(`Invalid job data for ${job.id}: ${JSON.stringify(job.data)}`);
			return;
		}

		let repo: Uint8Array | null;
		try {
			repo = Bun.mmap(path.join(process.env.REPOS_DIR!, did));
			if (!repo?.byteLength) throw "Got empty repo";
		} catch (err) {
			if (`${err}`.includes("ENOENT")) return;
			console.warn("Error while getting repo bytes for " + did, err);
			return;
		}

		try {
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

				(commitData[collection] ??= []).push(data);
			}
			await redis.sAdd("backfill:seen", did);
		} catch (err) {
			console.warn(`iterateAtpRepo error for did ${did} --- ${err}`);
			if (`${err}`.includes("invalid simple value")) {
				console.warn(`Marking broken bridgy repo ${did} as seen`);
				await redis.sAdd("backfill:seen", did);
			}
		} finally {
			repo = null;
			await fs.unlink(path.join(process.env.REPOS_DIR!, did));
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

	setTimeout(function sendCommits() {
		const entries = Object.entries(commitData);
		for (const [collection, commits] of entries) {
			process.send!({ type: "commit", collection, commits } satisfies CommitMessage);
			commitData[collection].length = 0;
		}
		commitData = {};
		setTimeout(sendCommits, 200);
		entries.length = 0;
	}, 200);

	setTimeout(function writeHS() {
		console.log("heap stats - repo - " + JSON.stringify(heapStats()));
		setTimeout(writeHS, 30_000);
	}, 30_000);
}
