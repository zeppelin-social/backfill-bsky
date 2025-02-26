import { fetchPdses } from "./util/fetch.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BGS_HOSTNAME: string;
			BGS_ADMIN_KEY: string;
		}
	}
}

for (const envVar of ["BGS_HOSTNAME", "BGS_ADMIN_KEY"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = (await fetchPdses()).map((url) => new URL(url));

	const bgs = "https://" + process.env.BGS_HOSTNAME;

	console.log("Requesting crawls...");
	await Promise.all(pdses.map(async (url) => {
		try {
			const res = await fetch(`${bgs}/admin/pds/requestCrawl`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${process.env.BGS_ADMIN_KEY}`,
				},
				body: JSON.stringify({
					hostname: "https://" + url.hostname,
					per_second: 200,
					per_hour: 150 * 60 * 60,
					per_day: 120 * 60 * 60 * 24,
					crawl_rate: 50,
					repo_limit: 1_000_000,
				}),
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
	console.log("Done crawling!");
}

void main();
