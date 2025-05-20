import {
	DidResolver,
	type DidResolverOpts,
	type IdentityResolverOpts,
	IdResolver as _IdResolver,
} from "@atproto/identity";
import { DidPlcResolver } from "@atproto/identity";
import { IndexingService as IdxService } from "@futuristick/atproto-bsky/dist/data-plane/server/indexing";
import { copyIntoTable } from "@futuristick/atproto-bsky/dist/data-plane/server/util";
import { setTimeout as sleep } from "node:timers/promises";
import PQueue from "p-queue";

export class IndexingService extends IdxService {
	override idResolver = new IdResolver();

	async indexActorsBulk(dids: Array<string>): Promise<void> {
		const actors: Array<{ did: string; handle: string; indexedAt: string }> = [];
		const now = new Date().toISOString();

		let aborted = false;
		const queue = new PQueue({ concurrency: 50 });
		await queue.addAll(dids.map((did) => async () => {
			try {
				if (aborted) return;
				const handle = await this.idResolver.resolveHandleCheck(did);
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
	constructor(opts?: IdentityResolverOpts & { fallbackPlc?: string | undefined }) {
		super(opts);
		if (opts?.fallbackPlc) {
			this.did = new FallbackPlcDidResolver({ ...opts, fallbackPlc: opts.fallbackPlc });
		}
	}

	async resolveHandleCheck(did: string): Promise<string | null> {
		// Try resolving did to handle, then handle back to did, and return resolved handle if they match
		const atpData = await this.did.resolveAtprotoData(did, true);
		const handleToDid = await this.handle.resolve(atpData.handle);
		let handle = did === handleToDid ? atpData.handle.toLowerCase() : null;

		// If they don't match, try doing the same thing with the fallback PLC resolver
		if (handle === null) {
			const fallbackAtpData = await this.did.methods.fallbackPlc.resolveAtprotoData(
				did,
				true,
			);
			// If it returns the same handle as the primary PLC resolver, we know it won't match
			if (fallbackAtpData.handle === atpData.handle) return null;
			const fallbackHandleToDid = await this.handle.resolve(fallbackAtpData.handle);
			if (did === fallbackHandleToDid) handle = fallbackAtpData.handle.toLowerCase();
		}

		return handle;
	}
}

class FallbackPlcDidResolver extends DidResolver {
	constructor({ fallbackPlc, ...opts }: DidResolverOpts & { fallbackPlc: string }) {
		super(opts);
		this.methods.fallbackPlc = new DidPlcResolver(fallbackPlc, opts.timeout ?? 3000);
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
