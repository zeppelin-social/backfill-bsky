import { getPdses } from "./backfillCommits.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BGS_URL: string;
		}
	}
}

for (const envVar of ["BGS_URL"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = await getPdses();

	for (const hostname of pdses) {
		const res = await fetch(`${process.env.BGS_URL}/pds/requestCrawl`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ hostname }),
		});

		if (!res.ok) {
			console.error(
				`Error requesting crawl for ${hostname}: ${res.status} ${res.statusText}`,
			);
		}
	}
}

void main();
