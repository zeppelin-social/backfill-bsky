import {
	type HeadersObject,
	type RPCOptions,
	XRPC as XRPCBase,
	XRPCError,
	type XRPCRequestOptions,
	type XRPCResponse,
} from "@atcute/client";
import type { Queries } from "@atcute/client/lexicons";
import CacheableLookup from "cacheable-lookup";
import cluster from "node:cluster";
import { Agent, setGlobalDispatcher } from "undici";
import { sleep } from "./util.js";

const cacheable = new CacheableLookup();

setGlobalDispatcher(
	new Agent({
		keepAliveTimeout: 300_000,
		connect: {
			timeout: 300_000,
			lookup: (hostname, { family: _family, hints, all, ..._options }, callback) => {
				const family = !_family ? undefined : (_family === 6 || _family === "IPv6") ? 6 : 4;
				return cacheable.lookup(hostname, {
					..._options,
					...(family ? { family } : {}),
					...(hints ? { hints } : {}),
					...(all ? { all } : {}),
				}, callback);
			},
		},
	}),
);

type OutputOf<T> = T extends { output: any } ? T["output"] : never;

const ratelimitCooldowns = new Map<string, Promise<any>>();

export class XRPC extends XRPCBase {
	constructor(public service: string) {
		super({ handler: (pathname, init) => fetch(new URL(pathname, this.service), init) });
	}

	override async request(options: XRPCRequestOptions, attempt = 0): Promise<XRPCResponse> {
		const url = this.service + "/xrpc/" + options.nsid;

		const cooldown = ratelimitCooldowns.get(url);
		if (cooldown) await cooldown;

		try {
			const res = await super.request(options);
			await processRatelimitHeaders(res.headers, url);
			return res;
		} catch (err) {
			if (attempt > 6) throw err;

			if (err instanceof XRPCError) {
				if (err.status === 429) await processRatelimitHeaders(err.headers, url);
				else throw err;
			} else if (err instanceof TypeError) {
				console.warn(`fetch failed for ${url}, skipping`);
				throw err;
			} else {
				await sleep(backoffs[attempt] || 60000);
			}
			console.warn(`retrying request to ${url}, on attempt ${attempt}`);
			return this.request(options, attempt + 1);
		}
	}

	override get<K extends keyof Queries>(
		nsid: K,
		options: RPCOptions<Queries[K]>,
	): Promise<XRPCResponse<OutputOf<Queries[K]>>> {
		return this.request({ type: "get", nsid: nsid, ...(options as any) });
	}
}

const backoffs = [1_000, 5_000, 15_000, 30_000, 60_000, 120_000, 300_000];

async function processRatelimitHeaders(headers: HeadersObject, url: string) {
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
			const waitTime = ratelimitReset - now + 1000; // add a second to be safe
			if (waitTime > 0) {
				const cooldown = sleep(waitTime);
				ratelimitCooldowns.set(url, cooldown);
				await cooldown;
			}
		}
	}
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
