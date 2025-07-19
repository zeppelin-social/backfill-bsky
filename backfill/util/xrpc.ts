import { Client, ok, simpleFetchHandler, type XRPCErrorPayload } from "@atcute/client";
import { setTimeout as sleep } from "node:timers/promises";
import { Agent, setGlobalDispatcher } from "undici";
import type {} from "@atcute/atproto";

type ExtractSuccessData<T> = T extends { ok: true; data: infer D } ? D : never;

type UnknownClientResponse =
	& { status: number; headers: Headers }
	& ({ ok: true; data: unknown } | { ok: false; data: XRPCErrorPayload });

const agent = new Agent({ pipelining: 0 });
setGlobalDispatcher(agent);

const retryableStatusCodes = new Set([408, 429, 503, 504]);
const maxRetries = 5;

export class XRPCManager {
	clients = new Map<string, Client>();

	async query<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
		attempt = 0,
	): Promise<ExtractSuccessData<T>> {
		try {
			return await this.queryNoRetry(service, fn);
		} catch (error) {
			if (await this.shouldRetry(error, attempt++)) {
				return await this.query(service, fn, attempt);
			}
			throw error;
		}
	}

	async queryNoRetry<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const client = this.getOrCreateClient(service);
		return await ok(fn(client));
	}

	createClient(service: string) {
		const client = new Client({ handler: simpleFetchHandler({ service }) });
		this.clients.set(service, client);
		return client;
	}

	getOrCreateClient(service: string) {
		return this.clients.get(service) ?? this.createClient(service);
	}

	private async shouldRetry(error: unknown, attempt = 0) {
		if (!error || typeof error !== "object") return false;

		const errorStr = `${error}`.toLowerCase();
		if (errorStr.includes("tcp") || errorStr.includes("network") || errorStr.includes("dns")) {
			return true;
		}

		if (error instanceof DOMException && error.name === "TimeoutError") return true;

		if (error instanceof TypeError) return false;

		if ("headers" in error && error.headers) {
			let reset;
			if (error.headers instanceof Headers && error.headers.has("ratelimit-reset")) {
				reset = parseInt(error.headers.get("ratelimit-reset")!);
			} else if (typeof error.headers === "object" && "ratelimit-reset" in error.headers) {
				reset = parseInt(`${error.headers["ratelimit-reset"]}`);
			}
			if (reset) {
				console.warn(`Rate limited, retrying in ${reset} seconds`, error);
				await sleep(reset * 1000 - Date.now());
				return true;
			}
		}

		if (attempt >= maxRetries) return false;

		if (
			"status" in error && typeof error.status === "number"
			&& retryableStatusCodes.has(error.status)
		) {
			const delay = Math.pow(3, attempt + 1);
			console.warn(`Retrying ${error.status} in ${delay} seconds`, error);
			await sleep(delay * 1000);
			return true;
		}

		return false;
	}
}
