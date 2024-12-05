import { fetchPdses } from "./util/fetch.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
			BGS_ADMIN_KEY: string;
		}
	}
}

for (const envVar of ["BSKY_REPO_PROVIDER", "BGS_ADMIN_KEY"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = await fetchPdses();

	const bgs = "https://" + process.env.BSKY_REPO_PROVIDER.replace(/^[a-z]+:\/\//, "");

	await Promise.all(pdses.map(async (_url) => {
		const url = new URL(_url);
		try {
			const res = await fetch(`${bgs}/admin/pds/requestCrawl`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${process.env.BGS_ADMIN_KEY}`,
				},
				body: JSON.stringify({ hostname: "https://" + url.hostname }),
			});
			if (!res.ok) {
				console.error(
					`Error requesting crawl for ${url.hostname}: ${res.status} ${res.statusText} — ${await res
						.json().then((r: any) => r?.error || "unknown error")}`,
				);
			}
		} catch (err) {
			console.error(`Network error requesting crawl for ${url.hostname}: ${err}`);
		}
	}));

	console.log("Done!");
}

void main();
