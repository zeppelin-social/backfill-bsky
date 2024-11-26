# backfill-bsky

Backfills a Bluesky AppView with historical commit data. This is expected to run on the same machine as the AppView, and will write directly to the database. Environment variables used are borrowed from [the bluesky-selfhost-env project](https://github.com/itaru2622/bluesky-selfhost-env/blob/467f060ab935d143096a7292c627a221cfef29b5/docker-compose.yaml#L294).

## Steps

0. Ensure BGS/relay is running.
1. `requestCrawlPdses.ts` — informs relay of all PDSes to be crawled to begin emitting events.
2. `bufferRelay.ts` — will run in the background until step 6 to buffer relay events while backfill is running.
3. `backfillCommits.ts` — will backfill the AppView historical commit data from all repos. Expected to take 15-18 hours as of Nov 26 2024 (~600k users per PDS on largest PDSes / 3000 requests per 300 seconds).
4. `sortBackfillData.ts` — will sort the backfilled data by timestamp.
5. `ingestBackfill.ts` — will ingest the backfilled data into the AppView.
6. `ingestBuffer.ts` — will ingest the buffered relay data into the AppView.
7. Restart AppView to start listening from the relay at cursor=0.
