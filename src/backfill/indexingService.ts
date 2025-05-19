import {
	DidResolver,
	type DidResolverOpts,
	type IdentityResolverOpts,
	IdResolver as _IdResolver,
} from "@atproto/identity";
import { IndexingService as IdxService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";
import { copyIntoTable } from "@futuristick/atproto-bsky/dist/data-plane/server/util";
import { setTimeout as sleep } from "node:timers/promises";
import PQueue from "p-queue";

export class IndexingService extends IdxService {
	async indexActorsBulk(dids: Array<string>): Promise<void> {
		const actors: Array<{ did: string; handle: string; indexedAt: string }> = [];
		const now = new Date().toISOString();

		let aborted = false;
		const queue = new PQueue({ concurrency: 50 });
		await queue.addAll(dids.map((did) => async () => {
			try {
				if (aborted) return;
				const atpData = await this.idResolver.did.resolveAtprotoData(did, true);
				const handleToDid = await this.idResolver.handle.resolve(atpData.handle);
				const handle = did === handleToDid ? atpData.handle.toLowerCase() : null;
				if (handle) actors.push({ did, handle, indexedAt: now });
			} catch (err) {
				if (err instanceof Error && "status" in err && err.status === 429) aborted = true;
			}
		}));
		if (aborted) {
			await sleep(60_000);
			return await this.indexActorsBulk(dids);
		}

		await copyIntoTable(this.db.pool, "actor", ["did", "handle", "indexedAt"], actors);
	}
}

export class IdResolver extends _IdResolver {
	constructor(
		{ fallbackPlc, ...opts }: IdentityResolverOpts & { fallbackPlc?: string | undefined },
	) {
		super(opts);
		if (fallbackPlc) this.did = new FallbackPlcDidResolver({ ...opts, fallbackPlc });
	}
}

class FallbackPlcDidResolver extends DidResolver {
	constructor({ fallbackPlc, ...opts }: DidResolverOpts & { fallbackPlc: string }) {
		super(opts);
		this.methods.fallbackPlc = new DidResolver({ plcUrl: fallbackPlc });
	}

	override async resolveNoCheck(did: string): Promise<unknown> {
		let initialRes, initialErr;
		try {
			initialRes = await super.resolveNoCheck(did);
		} catch (err) {
			initialErr = err;
		}

		if (initialRes) return initialRes;

		let fallbackRes, fallbackErr;
		try {
			fallbackRes = await this.methods.fallbackPlc.resolveNoCheck(did);
		} catch (err) {
			fallbackErr = err;
		}

		if (fallbackErr) {
			throw new Error(`Failed to resolve DID ${did}\n${initialErr}\n${fallbackErr}`);
		}
		return fallbackRes;
	}
}
