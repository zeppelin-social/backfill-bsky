export interface BackfillLine {
	action: "create";
	timestamp: number;
	uri: string;
	cid: string;
	record: unknown;
}

export async function getPdses(): Promise<Array<string>> {
	const atprotoScrapingData = await fetch(
		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
	).then((res) => res.ok ? res.json() : _throw("atproto-scraping state.json not ok")) as {
		pdses?: Record<string, unknown>;
	};

	if (!atprotoScrapingData.pdses) throw new Error("No pdses in atproto-scraping");

	const pdses = Object.keys(atprotoScrapingData.pdses).filter((pds) =>
		pds.startsWith("https://")
	);
	return pdses;
}

const _throw = (err: string) => {
	throw new Error(err);
};
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function* batch<T>(iterable: AsyncIterableIterator<T>, batchSize: number) {
	let items: T[] = [];
	for await (const item of iterable) {
		items.push(item);
		if (items.length >= batchSize) {
			yield items;
			items = [];
		}
	}
	if (items.length !== 0) {
		yield items;
	}
}
