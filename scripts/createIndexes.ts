import { Database } from "@futuristick/atproto-bsky";
import fs from "node:fs";

const db = new Database({
	url: process.env.BSKY_DB_POSTGRES_URL,
	schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
});

const createCmd = fs.readFileSync("create.sql", "utf-8");
await db.pool.query(createCmd);
