import { iterateAtpRepo } from "@atcute/car";
import { CredentialManager, type HeadersObject, XRPC, XRPCError } from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import * as fs from "node:fs";
import { type BackfillLine, getPdses, sleep } from "./shared.js";

async function main() {
	const ws = fs.createWriteStream("backfill-unsorted.jsonl");

	let seenDids: Record<string, Record<string, boolean>>;
	try {
		seenDids = JSON.parse(fs.readFileSync("seen-dids.json", "utf-8"));
	} catch {
		seenDids = {};
	}

	setInterval(() => {
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	}, 30_000);

	const onFinish = () => {
		ws.close();
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	};

	process.on("SIGINT", onFinish);
	process.on("SIGTERM", onFinish);
	process.on("exit", onFinish);

	const pdses = await getPdses();

	await Promise.allSettled(pdses.map(async (pds) => {
		try {
			seenDids[pds] ??= {};
			for await (const did of listRepos(pds)) {
				if (seenDids[pds][did]) continue;
				try {
					const repo = await getRepo(pds, did);
					if (!repo) continue;
					for await (const { record, rkey, collection, cid } of iterateAtpRepo(repo)) {
						const uri = `at://${did}/${collection}/${rkey}`;
						let timestamp: number;
						try {
							timestamp = parseTID(rkey).timestamp;
						} catch {
							timestamp =
								record && typeof record === "object" && "createdAt" in record
									&& typeof record.createdAt === "string"
									? new Date(record.createdAt).getTime()
									: Date.now();
						}
						const line: BackfillLine = {
							action: "create",
							timestamp,
							uri,
							cid: cid.$link,
							record,
						};
						ws.write(JSON.stringify(line) + "\n");
					}
					seenDids[pds][did] = true;
				} catch (err) {
					console.warn(`Skipping repo ${did} for pds ${pds}: ${err}`);
				}
			}
			console.log(`Finished processing ${pds}`);
			return pds;
		} catch (err) {
			console.warn(`Skipping pds ${pds}: ${err}`);
		}
	}));

	onFinish();
}

async function getRepo(pds: string, did: `did:${string}`) {
	const rpc = new XRPC({ handler: new CredentialManager({ service: pds }) });

	try {
		const { data, headers } = await rpc.get("com.atproto.sync.getRepo", { params: { did } });
		await parseRatelimitHeadersAndWaitIfNeeded(headers, pds);
		return data;
	} catch (err) {
		if (err instanceof XRPCError && err.status === 429) {
			await parseRatelimitHeadersAndWaitIfNeeded(err.headers, pds);
		} else throw err;
	}
}

async function* listRepos(pds: string) {
	let cursor: string | undefined = "";
	const rpc = new XRPC({ handler: new CredentialManager({ service: pds }) });
	do {
		try {
			const { data: { repos, cursor: newCursor }, headers } = await rpc.get(
				"com.atproto.sync.listRepos",
				{ params: { limit: 1000, cursor } },
			);

			cursor = newCursor as string | undefined; // I do not know why this is necessary but the previous line errors otherwise

			for (const repo of repos) {
				yield repo.did;
			}

			await parseRatelimitHeadersAndWaitIfNeeded(headers, pds);
		} catch (err) {
			if (err instanceof XRPCError && err.status === 429) {
				await parseRatelimitHeadersAndWaitIfNeeded(err.headers, pds);
			} else throw err;
		}
	} while (cursor);
}

async function parseRatelimitHeadersAndWaitIfNeeded(headers: HeadersObject, pds: string) {
	const remainingHeader = headers["ratelimit-remaining"],
		resetHeader = headers["ratelimit-reset"];
	if (!remainingHeader || !resetHeader) return;

	const ratelimitRemaining = parseInt(remainingHeader);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
		const ratelimitReset = parseInt(resetHeader) * 1000;
		if (isNaN(ratelimitReset)) {
			throw new Error("ratelimit-reset header is not a number for pds " + pds);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 3000; // add 3s to be safe
			if (waitTime > 0) {
				await sleep(waitTime);
			}
		}
	}
}

void main();
