# backfill-bsky

Backfills a Bluesky AppView with historical commit data. This is expected to run on the same machine as the AppView, and will write directly to the database.

### Environment variables

- `BSKY_DB_POSTGRES_URL` — a Postgres connection string to the AppView database.
- `BSKY_DB_POSTGRES_SCHEMA` — the schema name for the database; most likely `bsky`.
- `BSKY_DID_PLC_URL` — the URL of the PLC directory to use for resolving DIDs. If you're self hosting a PLC mirror, this can be the mirror's URL; otherwise, it should be `https://plc.directory`.
- (optional) `FALLBACK_PLC_URL` — the URL of a fallback PLC directory to use; you can set this to `https://plc.directory` if you're self hosting a PLC mirror.
- `BUFFER_REPO_PROVIDER` — a `wss://` URL to the relay to buffer events from while backfilling.
- `OPENSEARCH_URL` — an optional URL to an OpenSearch instance to backfill as well.
- `OPENSEARCH_USERNAME` — a username to authenticate with OpenSearch. If you're running [zeppelin-social/bluesky-appview](https://github.com/zeppelin-social/bluesky-appview), this is probably `admin`.
- `OPENSEARCH_PASSWORD` — a password to authenticate with OpenSearch.

## Steps

0. Ensure the AppView is running.
1. `bun drop-indexes` — will drop all indexes from the AppView database and log the command to recreate them to stdout and `create.sql`.
    - Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`
2. `bun buffer` — will run in the background until step 4 to buffer relay events while backfill is running.
    - Requires: `BUFFER_REPO_PROVIDER` — a `wss://` URL to the relay.
    - Recommended to run with `pm2` or `forever` to ensure it runs in the background.
3. `bun backfill` — will backfill the AppView with historical commit data from all repos. Expected to take about 3 days as of July 23 2025.
   - Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`, `BSKY_DID_PLC_URL`, (optional) `FALLBACK_PLC_URL`, (optional) `OPENSEARCH_URL`, (optional) `OPENSEARCH_USERNAME`, (optional) `OPENSEARCH_PASSWORD`
   - Recommended to run with `pm2` or `forever` to ensure it runs in the background.
4. `bun create-indexes` — will recreate the indexes in the AppView database.
    - Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`
5. Restart the AppView to start listening from the relay at cursor=0, filling in any gaps in the buffer within the relay replay window.
6. `bun buffer:ingest` — will ingest the buffered relay data into the AppView.
    - Requires: `BSKY_DB_POSTGRES_URL`, `BSKY_DB_POSTGRES_SCHEMA`, `BSKY_DID_PLC_URL`
