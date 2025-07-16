import {
	DidResolver,
	type DidResolverOpts,
	type IdentityResolverOpts,
	IdResolver as _IdResolver,
} from "@atproto/identity";
import { DidPlcResolver } from "@atproto/identity";
import { IndexingService as IdxService } from "@zeppelin-social/bsky-backfill/dist/data-plane/server/indexing";
import { copyIntoTable } from "@zeppelin-social/bsky-backfill/dist/data-plane/server/util";
import { setTimeout as sleep } from "node:timers/promises";
import PQueue from "p-queue";

type ActorToIndex = { did: string; handle: string; indexedAt: string };

export class IndexingService extends IdxService {
	override idResolver = new IdResolver();

	async indexActorsBulk(dids: string[]): Promise<void> {
		const actors: ActorToIndex[] = [];
		const toRetry: string[] = [];
		const now = new Date().toISOString();

		let aborted = false;
		const queue = new PQueue({ concurrency: 50 });
		await queue.addAll(dids.map((did) => async () => {
			try {
				if (aborted) return toRetry.push(did);
				const handle = await this.idResolver.resolveHandleNoCheck(did);
				if (handle) actors.push({ did, handle, indexedAt: now });
			} catch (err) {
				if (err instanceof Error && "status" in err && err.status === 429) {
					aborted = true;
					toRetry.push(did);
				}
			}
		}));

		await copyIntoTable(this.db.pool, "actor", ["did", "handle", "indexedAt"], actors);

		if (aborted) {
			await sleep(60_000);
			return await this.indexActorsBulk(toRetry);
		}
	}
}

export class IdResolver extends _IdResolver {
	constructor(opts?: IdentityResolverOpts & { fallbackPlc?: string | undefined }) {
		super(opts);
		if (opts?.fallbackPlc) {
			this.did = new FallbackPlcDidResolver({ ...opts, fallbackPlc: opts.fallbackPlc });
		}
	}

	async resolveHandleNoCheck(did: string): Promise<string | null> {
		// This would ideally query the handle URL to make sure it points back to the DID,
		// but an extra query per DID is expensive. Duplicate actors will fail insertion
		// and be re-indexed by the AppView whenever they next update their identity.
		const atpData = await this.did.resolveAtprotoData(did, true);
		const handle = atpData.handle.toLowerCase();
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
