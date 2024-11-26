import { getPdses } from './shared.js'

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
		}
	}
}

for (const envVar of ["BSKY_REPO_PROVIDER"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = await getPdses();

	const bgs = "https://" + process.env.BSKY_REPO_PROVIDER.replace(/^[a-z]+:\/\//, "");

	for (const hostname of pdses) {
		const res = await fetch(`${bgs}/xrpc/com.atproto.sync.requestCrawl`, {
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
