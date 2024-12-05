import { iterateAtpRepo } from "@atcute/car";
import { parse as parseTID } from "@atcute/tid";
import { parentPort } from "worker_threads";
import type { CommitData, RepoWorkerMessage } from "./main.js";

parentPort!.on("message", async (message: RepoWorkerMessage) => {
	if (!message.did || !message.repoBytes) {
		throw new Error("Invalid repo received: " + JSON.stringify(message));
	}

	const out = [];
	const now = Date.now();
	for await (const { record, rkey, collection, cid } of iterateAtpRepo(message.repoBytes)) {
		const uri = `at://${message.did}/${collection}/${rkey}`;

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

		const commit = {
			uri,
			cid: cid.$link,
			indexedAt: new Date(indexedAt).toISOString(),
			record,
		} satisfies CommitData;
		out.push(commit);
	}
	parentPort!.postMessage(out);
});
