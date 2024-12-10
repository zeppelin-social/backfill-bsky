# backfill-bsky

Backfills a Bluesky AppView with historical commit data. This is expected to run on the same machine as the AppView, and will write directly to the database. Environment variables used are borrowed from [the bluesky-selfhost-env project](https://github.com/itaru2622/bluesky-selfhost-env/blob/467f060ab935d143096a7292c627a221cfef29b5/docker-compose.yaml#L294).

## Steps

0. Ensure AppView and BGS/relay are running, `pnpm build`.
1. `pnpm request-crawl` — informs relay of all PDSes to be crawled to begin emitting events.
	- Requires: `BSKY_REPO_PROVIDER`, `BGS_ADMIN_KEY`
2. `pnpm buffer` — will run in the background until step 4 to buffer relay events while backfill is running.
	- Requires: `BSKY_REPO_PROVIDER`
3. `pnpm backfill` — will backfill the AppView historical commit data from all repos. Expected to take 15-18 hours as of Nov 26 2024 (~600k users per PDS on largest PDSes / 3000 requests per 300 seconds).
   - Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`, `BSKY_REPO_PROVIDER`, `BSKY_DID_PLC_URL`
4. `pnpm buffer:ingest` — will ingest the buffered relay data into the AppView.
	- Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`, `BSKY_REPO_PROVIDER`, `BSKY_DID_PLC_URL`
5. Restart AppView to start listening from the relay at cursor=0, filling in any gaps in the buffer within the relay replay window.
