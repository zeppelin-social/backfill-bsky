import { iterateAtpRepo } from "@atcute/car";
import {
	CredentialManager,
	type HeadersObject,
	XRPC,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import { parse as parseTID } from "@atcute/tid";
import * as fs from "node:fs";
import { Agent, fetch, setGlobalDispatcher } from "undici";
import { type BackfillLine, getPdses, sleep } from "./shared.js";

setGlobalDispatcher(new Agent({ keepAliveTimeout: 60_000, connect: { timeout: 60_000 } }));

async function main() {
	const ws = fs.createWriteStream("backfill-unsorted.jsonl", { flags: "a+" });

	let seenDids: Record<string, Record<string, boolean>>;
	try {
		seenDids = JSON.parse(fs.readFileSync("seen-dids.json", "utf-8"));
	} catch {
		seenDids = {};
	}

	setInterval(() => {
		fs.writeFileSync("seen-dids.json", JSON.stringify(seenDids));
	}, 20_000);

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
				const repo = await getRepo(pds, did);
				if (!repo) continue;
				try {
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
				} catch (err) {
					console.warn(`iterateAtpRepo error for did ${did} from pds ${pds} --- ${err}`);
				}
				seenDids[pds][did] = true;
			}
			console.log(`Finished processing ${pds}`);
		} catch (err) {
			console.warn(`Unknown error, skipping pds ${pds} --- ${err}`);
		}
	}));

	onFinish();
}

async function getRepo(pds: string, did: `did:${string}`) {
	const rpc = new WrappedRPC(pds);
	try {
		const { data } = await rpc.get("com.atproto.sync.getRepo", { params: { did } });
		return data;
	} catch (err) {
		console.error(`getRepo error for did ${did} from pds ${pds} --- ${err}`);
	}
}

async function* listRepos(pds: string) {
	let cursor: string | undefined = "";
	const rpc = new WrappedRPC(pds);
	do {
		try {
			const { data: { repos, cursor: newCursor } } = await rpc.get(
				"com.atproto.sync.listRepos",
				{ params: { limit: 1000, cursor } },
			);

			cursor = newCursor as string | undefined; // I do not know why this is necessary but the previous line errors otherwise

			for (const repo of repos) {
				yield repo.did;
			}
		} catch (err) {
			console.error(`listRepos error for pds ${pds} at cursor ${cursor} --- ${err}`);
			return;
		}
	} while (cursor);
}

async function parseRatelimitHeadersAndWaitIfNeeded(headers: HeadersObject, url: string) {
	const remainingHeader = headers["ratelimit-remaining"],
		resetHeader = headers["ratelimit-reset"];
	if (!remainingHeader || !resetHeader) return;

	const ratelimitRemaining = parseInt(remainingHeader);
	if (isNaN(ratelimitRemaining) || ratelimitRemaining <= 1) {
		const ratelimitReset = parseInt(resetHeader) * 1000;
		if (isNaN(ratelimitReset)) {
			console.error("ratelimit-reset header is not a number at url " + url);
		} else {
			const now = Date.now();
			const waitTime = ratelimitReset - now + 2000; // add 2s to be safe
			if (waitTime > 0) {
				await sleep(waitTime);
			}
		}
	}
}

const backoffs = [1000, 2500, 5000, 15000, 30000, 60000, 120000];

class WrappedRPC extends XRPC {
	constructor(public service: string) {
		// @ts-expect-error undici version mismatch causing fetch type incompatibility
		super({ handler: new CredentialManager({ service, fetch }) });
	}
	override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
		const url = this.service + "/xrpc/" + options.nsid;

		const request = async () => {
			const res = await super.request(options);
			await parseRatelimitHeadersAndWaitIfNeeded(res.headers, url);
			return res;
		};

		try {
			return await request();
		} catch (err) {
			if (attempt > 6) throw err;

			if (err instanceof XRPCError) {
				if (err.status === 429) {
					await parseRatelimitHeadersAndWaitIfNeeded(err.headers, url);
				} else throw err;
			} else {
				if (err instanceof TypeError) console.log(err);
				await sleep(backoffs[attempt] || 60000);
			}
			return this.request(options, attempt + 1);
		}
	}
}

void main();
