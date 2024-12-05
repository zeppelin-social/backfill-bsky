export async function fetchPdses(): Promise<Array<string>> {
	const data = await fetch(
		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
	).then((res) => res.ok ? res.json() as any : null);

	if (!data.pdses) throw new Error("Failed to fetch PDSes");

	const pdses = Object.keys(data.pdses).filter((pds) => pds.startsWith("https://"));
	return pdses;
}

export async function fetchAllRepos(): Promise<Map<string, string>> {
	const map = new Map<string, string>();
	await fetchPlcDids(map);
	await fetchWebDids(map);
	return map;
}

async function fetchPlcDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
	let cursor = "";
	do {
		const res = await fetch(`https://plc.directory/export?limit=1000%after=${cursor}`);
		if (!res.ok) {
			throw new Error(`Failed to fetch PLC DIDs: ${res.status} ${res.statusText}`);
		}

		const lines = await res.text();
		const operations = lines.split("\n").map((line) => {
			try {
				return JSON.parse(line);
			} catch (e) {
				return null;
			}
		});

		for (const op of operations) {
			if (!op?.operation?.type) continue;
			if (op.operation.type === "create" && op.operation.service) {
				map.set(op.did, op.operation.service);
			} else if (op.operation.type === "plc_operation") {
				const pds = op.operation.services.atproto_pds.endpoint;
				if (pds) map.set(op.did, pds);
			} else if (op.operation.type === "plc_tombstone") map.delete(op.did);
		}

		cursor = operations.at(-1)?.createdAt;
	} while (cursor);

	return map;
}

async function fetchWebDids(map: Map<string, string> = new Map()): Promise<Map<string, string>> {
	const data = await fetch(
		"https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json",
	).then((res) => res.ok ? res.json() as any : null);
	if (!data?.firehose?.didWebs) throw new Error("Failed to fetch web DIDs");
	for (const [did, { pds }] of Object.entries<{ pds: string }>(data.firehose.didWebs)) {
		map.set(did, pds);
	}
	return map;
}
