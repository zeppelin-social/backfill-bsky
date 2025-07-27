import { MemoryCache } from "@atproto/identity";
import { LRUCache } from "lru-cache";

export class LRUDidCache extends MemoryCache {
	constructor(max: number) {
		super();
		// @ts-expect-error — close enough to a Map for our purposes
		this.cache = new LRUCache({ max });
	}
}
