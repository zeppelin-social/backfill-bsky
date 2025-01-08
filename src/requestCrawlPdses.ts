import { Client } from "pg";
import { fetchPdses } from "./util/fetch.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BGS_HOSTNAME: string;
			BSKY_DB_POSTGRES_URL: string;
			BGS_ADMIN_KEY: string;
		}
	}
}

for (const envVar of ["BGS_HOSTNAME", "BSKY_DB_POSTGRES_URL", "BGS_ADMIN_KEY"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = (await fetchPdses()).map((url) => new URL(url));

	const bgs = "https://" + process.env.BGS_HOSTNAME;

	if (!process.argv.includes("--no-crawl")) {
		console.log("Requesting crawls...");
		await Promise.all(pdses.map(async (url) => {
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
		console.log("Done crawling!");
	}

	if (!process.argv.includes("--no-change-limits")) {
		console.log("Setting rate limits...");
		try {
			const db = new Client({
				connectionString: process.env.BSKY_DB_POSTGRES_URL.replace("bsky", "bgs"),
			});

			await db.query(
				`
				update pds set
					rate_limit = $1,
					crawl_rate_limit = $2,
					repo_limit = $3,
					hourly_event_limit = $4,
					daily_event_limit = $5
				where 1 = 1;
			`,
				[2000, 50, 1000000, 3000000, 20000000],
			);
		} catch (err) {
			console.error(`Error setting rate limits: ${err}`);
		}
		console.log("Done setting rate limits!");
	}
}

void main();
