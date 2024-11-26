import { getPdses } from "./shared.js";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
		}
	}
}

for (const envVar of ["BSKY_REPO_PROVIDER", "BGS_ADMIN_KEY"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

async function main() {
	const pdses = await getPdses();

	const bgs = "https://" + process.env.BSKY_REPO_PROVIDER.replace(/^[a-z]+:\/\//, "");

	await Promise.all(pdses.map(async (url) => {
		const hostname = "https://" + new URL(url).hostname;
		try {
			const res = await fetch(`${bgs}/admin/pds/requestCrawl`, {
				method: "POST",
				headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.BGS_ADMIN_KEY}` },
				body: JSON.stringify({ hostname }),
			});
			if (!res.ok) {
				console.error(
					`Error requesting crawl for ${hostname}: ${res.status} ${res.statusText} — ${await res
						.json().then((r) => (r as any).error)}`,
				);
			}
		} catch (err) {
			console.error(`Network error requesting crawl for ${hostname}: ${err}`);
		}
	}));
}

void main();
