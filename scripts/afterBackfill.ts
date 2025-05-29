import { jsonStringToLex } from "@atproto/lexicon";
import { Database } from "@futuristick/atproto-bsky";
import type {
	DatabaseSchema,
	DatabaseSchemaType,
} from "@futuristick/atproto-bsky/dist/data-plane/server/db/database-schema";
import {
	executeRaw,
	invalidReplyRoot,
	violatesThreadGate,
} from "@futuristick/atproto-bsky/dist/data-plane/server/util";
import type {
	Record as PostRecord,
	ReplyRef,
} from "@futuristick/atproto-bsky/dist/lexicon/types/app/bsky/feed/post";
import type { Record as PostgateRecord } from "@futuristick/atproto-bsky/dist/lexicon/types/app/bsky/feed/postgate";
import type { Record as GateRecord } from "@futuristick/atproto-bsky/dist/lexicon/types/app/bsky/feed/threadgate";
import { postUriToThreadgateUri, uriToDid } from "@futuristick/atproto-bsky/dist/util/uris";
import { parsePostgate } from "@futuristick/atproto-bsky/dist/views/util";
import { sql } from "kysely";
import fs from "node:fs";
import path from "node:path";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_DB_POSTGRES_URL: string;
			BSKY_DB_POSTGRES_SCHEMA: string;
			BSKY_DID_PLC_URL: string;
		}
	}
}

for (const envVar of ["BSKY_DB_POSTGRES_URL", "BSKY_DB_POSTGRES_SCHEMA", "BSKY_DID_PLC_URL"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

const statePath = path.join(process.cwd(), "after-backfill-state.json");
let state: State = { postCursor: null, profileCursor: null, validationIndex: 0 };

const POOL_SIZE = 10;

// const DB_SETTINGS = {
// 	max_parallel_workers: 24,
// 	max_parallel_workers_per_gather: 24,
// 	max_worker_processes: 32,
// 	maintenance_work_mem: "\"32GB\"",
// };

async function main() {
  let [postCursor, profileCursor, _validationOffset]: Array<string | null> = process.argv.slice(2);


	const state = loadState();
	postCursor ??= state.postCursor;
	profileCursor ??= state.profileCursor;
	const validationOffset = (_validationOffset && !isNaN(parseInt(_validationOffset))) ? parseInt(_validationOffset) : state.validationIndex;

	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: POOL_SIZE,
	});

	await alterDbSettings(db);
	addExitHandlers(db);

	console.log("beginning backfill...");

	await Promise.allSettled([
		backfillPostAggregates(db, postCursor),
		backfillProfileAggregates(db, profileCursor),
	]);
	await backfillPostValidation(db, validationOffset);
}

void main();

async function backfillPostAggregates({ db }: Database, cursor: string | null = null) {
	const limit = 10_000;
	let rowCount = await fastRowCount(db, "post");
	console.log(`post row count: ${rowCount}`);

	let batches = Math.ceil(rowCount / limit);
	let i = 0;
	try {
		while (true) {
			if (i >= batches) {
				rowCount = await fastRowCount(db, "post");
				batches = Math.ceil(rowCount / limit);
			}

			saveState((s) => ({ ...s, postCursor: cursor }));

			console.time(`backfilling posts ${i + 1}/${batches}`);

			const inserted = await sql`
   	  WITH inserted AS (
        WITH posts (uri, cid) AS (
            SELECT uri, cid
            FROM post
            WHERE uri IS NOT NULL
              AND cid IS NOT NULL
              AND (${cursor} IS NULL OR uri > ${cursor})
            ORDER BY uri ASC
            LIMIT ${limit}
        )
        INSERT INTO post_agg ("uri", "replyCount", "likeCount", "repostCount", "quoteCount")
        SELECT
            p.uri,
            COALESCE(replies.count, 0) as "replyCount",
            COALESCE(likes.count, 0) as "likeCount",
            COALESCE(reposts.count, 0) as "repostCount",
            COALESCE(quotes.quoteCount, 0) as "quoteCount"
        FROM posts p
        LEFT JOIN (
            SELECT "replyParent" as uri, COUNT(*) as count
            FROM post
            WHERE "replyParent" IN (SELECT uri FROM posts)
                AND (post."violatesThreadGate" IS NULL OR post."violatesThreadGate" = false)
            GROUP BY "replyParent"
        ) replies ON replies.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as count
            FROM "like"
            WHERE subject IN (SELECT uri FROM posts)
            GROUP BY subject
        ) likes ON likes.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as count
            FROM repost
            WHERE subject IN (SELECT uri FROM posts)
            GROUP BY subject
        ) reposts ON reposts.uri = p.uri
        LEFT JOIN (
            SELECT subject as uri, COUNT(*) as quoteCount
            FROM quote
            WHERE subject IN (SELECT uri FROM posts)
            AND "subjectCid" IN (SELECT cid FROM posts WHERE quote.subject = posts.uri)
            GROUP BY subject
        ) quotes ON quotes.uri = p.uri
        ON CONFLICT (uri) DO UPDATE
            SET "replyCount" = excluded."replyCount",
                "likeCount" = excluded."likeCount",
                "repostCount" = excluded."repostCount",
                "quoteCount" = excluded."quoteCount"
        RETURNING uri
      ),
      batch_info AS (
          SELECT uri
          FROM post
          WHERE uri IS NOT NULL
            AND cid IS NOT NULL
            AND (${cursor} IS NULL OR uri > ${cursor})
          ORDER BY uri ASC
          LIMIT ${limit}
      )
      SELECT
          COUNT(inserted.uri) as processed_count,
          MAX(batch_info.uri) as next_cursor,
          MIN(batch_info.uri) as batch_start,
          MAX(batch_info.uri) as batch_end
      FROM inserted
      FULL OUTER JOIN batch_info ON inserted.uri = batch_info.uri;
			`.execute(db);

			if (inserted.rows.length === 0) break;
			// @ts-expect-error — row is not typed
			if (inserted.rows[0].processed_count === 0) break;
			// @ts-expect-error — row is not typed
			cursor = inserted.rows[0].next_cursor;

			console.timeEnd(`backfilling posts ${i + 1}/${batches}`);
			i++;
		}
	} catch (err) {
		console.error(`backfilling posts ${i + 1}/${batches}`, err);
		if (err instanceof Error && err.stack) console.error(err.stack);
	}
}

async function backfillProfileAggregates({ db }: Database, cursor: string | null = null) {
  const limit = 1_000;
	let rowCount = await fastRowCount(db, "actor");
	console.log(`actor row count: ${rowCount}`);

	let batches = Math.ceil(rowCount / limit);
  let i = 0;
	try {
		while (true) {
			if (i >= batches) {
				rowCount = await fastRowCount(db, "profile");
				batches = Math.ceil(rowCount / limit);
			}

			saveState((s) => ({ ...s, profileCursor: cursor }));

			console.time(`backfilling profiles ${i + 1}/${batches}`);

			const inserted = await sql`
      WITH batch_query AS (
        SELECT
          actor.did as creator,
          ROW_NUMBER() OVER (ORDER BY actor.did ASC) as rn
        FROM actor
        WHERE actor.did IS NOT NULL
          AND (${cursor} IS NULL OR ${cursor} = '' OR actor.did > ${cursor})
        ORDER BY actor.did ASC
        LIMIT ${limit}
      ),
      batch_profiles AS (
        SELECT creator FROM batch_query
      ),
      followers_counts AS (
        SELECT f."subjectDid" as did, COUNT(*) as cnt
        FROM follow f
        WHERE f."subjectDid" IN (SELECT creator FROM batch_profiles)
        GROUP BY f."subjectDid"
      ),
      follows_counts AS (
        SELECT f.creator as did, COUNT(*) as cnt
        FROM follow f
        WHERE f.creator IN (SELECT creator FROM batch_profiles)
        GROUP BY f.creator
      ),
      posts_counts AS (
        SELECT p.creator as did, COUNT(*) as cnt
        FROM post p
        WHERE p.creator IN (SELECT creator FROM batch_profiles)
        GROUP BY p.creator
      ),
      insert_result AS (
        INSERT INTO profile_agg ("did", "followersCount", "followsCount", "postsCount")
        SELECT
          bp.creator,
          COALESCE(fl.cnt, 0),
          COALESCE(fo.cnt, 0),
          COALESCE(p.cnt, 0)
        FROM batch_profiles bp
        LEFT JOIN followers_counts fl ON fl.did = bp.creator
        LEFT JOIN follows_counts fo ON fo.did = bp.creator
        LEFT JOIN posts_counts p ON p.did = bp.creator
        ON CONFLICT (did) DO UPDATE
        SET "followersCount" = excluded."followersCount",
            "followsCount" = excluded."followsCount",
            "postsCount" = excluded."postsCount"
        RETURNING did
      )
      SELECT
        MAX(creator) as next_cursor,
        COUNT(*) as processed_count,
        MIN(creator) as batch_start,
        MAX(creator) as batch_end
      FROM batch_profiles;
			`.execute(db);

			if (inserted.rows.length === 0) break;
			// @ts-expect-error — row is not typed
			if (inserted.rows[0].processed_count === 0) break;
			// @ts-expect-error — row is not typed
      cursor = inserted.rows[0].next_cursor;

			console.timeEnd(`backfilling posts ${i + 1}/${batches}`);
			i++;
		}
	} catch (err) {
		console.error(`backfilling posts ${i + 1}/${batches}`, err);
		if (err instanceof Error && err.stack) console.error(err.stack);
	}
}

async function backfillPostValidation({ db }: Database, offset?: number | undefined) {
	const limit = 10_000;

	let rowCount = await fastRowCount(db, "post");
	console.log(`post row count: ${rowCount}`);

	let batches = Math.ceil(rowCount / limit);
	let i = offset ?? 0;
	try {
		while (true) {
			if (i >= batches) {
				rowCount = await fastRowCount(db, "post");
				batches = Math.ceil(rowCount / limit);
			}

			saveState((s) => ({ ...s, validationIndex: i }));
			const offset = i * limit;

			const invalidReplyUpdates: Array<
				[uri: string, invalidReplyRoot: boolean, violatesThreadGate: boolean]
			> = [];

			console.time(`validating posts ${i + 1}/${batches}`);
			const posts = await db.selectFrom("post").innerJoin(
				"post_embed_record as embed",
				"embed.postUri",
				"uri",
			).select([
				"replyParent",
				"replyParentCid",
				"replyRoot",
				"replyRootCid",
				"creator",
				"uri",
				"embed.embedUri as embedUri",
			]).where("replyParent", "is not", null).where("replyRoot", "is not", null).orderBy(
				"uri",
				"asc",
			).limit(limit).offset(offset).execute();

			if (posts.length === 0) break;

			await Promise.all([validateReplyStatus(), validateEmbeddingRules()]);

			async function validateReplyStatus() {
				console.time(`validating reply status ${i + 1}/${batches}`);

				for (const post of posts) {
					if (
						!post.replyParent || !post.replyParentCid || !post.replyRoot
						|| !post.replyRootCid
					) continue;
					try {
						const { invalidReplyRoot, violatesThreadGate } = await validateReply(
							db,
							post.creator,
							{
								parent: { uri: post.replyParent, cid: post.replyParentCid },
								root: { uri: post.replyRoot, cid: post.replyRootCid },
							},
						);
						if (invalidReplyRoot || violatesThreadGate) {
							invalidReplyUpdates.push([
								post.uri,
								invalidReplyRoot,
								violatesThreadGate,
							]);
						}
					} catch (err) {
						console.error(`validating post ${post.uri}`, err);
						if (err instanceof Error && err.stack) console.error(err.stack);
					}
				}

				await executeRaw(
					db,
					`
					UPDATE post SET "invalidReplyRoot" = v."invalidReplyRoot", "violatesThreadGate" = v."violatesThreadGate"
					FROM (
						SELECT * FROM unnest($1::text[], $2::boolean[], $3::boolean[]) AS t(uri, "invalidReplyRoot", "violatesThreadGate")
					) as v
					WHERE post.uri = v.uri
					`,
					invalidReplyUpdates,
				);

				console.timeEnd(`validating reply status ${i + 1}/${batches}`);
			}

			async function validateEmbeddingRules() {
				console.time(`validating embedding rules ${i + 1}/${batches}`);

				const embeds: Array<{ parentUri: string; embedUri: string }> = [];
				const violatesEmbeddingRulesUpdates: Array<
					[uri: string, embedUri: string, violatesEmbeddingRules: boolean]
				> = [];

				for (const post of posts) {
					if (post.embedUri) {
						embeds.push({ parentUri: post.uri, embedUri: post.embedUri });
					}
				}

				const embedsToUpdate = await validatePostEmbedsBulk(db, embeds);
				for (const embed of embedsToUpdate) {
					if (embed.violatesEmbeddingRules) {
						violatesEmbeddingRulesUpdates.push([
							embed.parentUri,
							embed.embedUri,
							embed.violatesEmbeddingRules,
						]);
					}
				}

				if (violatesEmbeddingRulesUpdates.length) {
					await executeRaw(
						db,
						`
						UPDATE post SET "violatesEmbeddingRules" = v."violatesEmbeddingRules"
						FROM (
							SELECT * FROM unnest($1::text[], $2::boolean[]) AS t(uri, "violatesEmbeddingRules")
						) as v
						WHERE post.uri = v.uri
						`,
						violatesEmbeddingRulesUpdates,
					);
				}

				console.timeEnd(`validating embedding rules ${i + 1}/${batches}`);
			}

			console.timeEnd(`validating posts ${i + 1}/${batches}`);
			i++;
		}
	} catch (err) {
		console.error(`validating posts ${i + 1}/${batches}`, err);
		if (err instanceof Error && err.stack) console.error(err.stack);
	}
}

async function validateReply(db: DatabaseSchema, creator: string, reply: ReplyRef) {
	const replyRefs = await getReplyRefs(db, reply);
	const invalidRoot = !replyRefs.parent || invalidReplyRoot(reply, replyRefs.parent);
	const violatesGate = await violatesThreadGate(
		db,
		creator,
		uriToDid(reply.root.uri),
		replyRefs.root?.record ?? null,
		replyRefs.gate?.record ?? null,
	);
	return { invalidReplyRoot: invalidRoot, violatesThreadGate: violatesGate };
}

async function validatePostEmbedsBulk(
	db: DatabaseSchema,
	embeds: Array<{ parentUri: string; embedUri: string }>,
) {
	const uris = embeds.reduce((acc, { parentUri, embedUri }) => {
		const postgateRecordUri = embedUri.replace("app.bsky.feed.post", "app.bsky.feed.postgate");
		acc[postgateRecordUri] = { parentUri, embedUri };
		return acc;
	}, {} as Record<string, { parentUri: string; embedUri: string }>);

	const { rows: postgateRecords } = await executeRaw<DatabaseSchemaType["record"]>(
		db,
		`
    SELECT * FROM record WHERE record.uri = ANY($1::text[])
    `,
		[Object.keys(uris)],
	);

	return postgateRecords.reduce((acc, record) => {
		if (!record.json || !uris[record.uri]) return acc;
		const { embeddingRules: { canEmbed } } = parsePostgate({
			gate: jsonStringToLex(record.json) as PostgateRecord,
			viewerDid: uriToDid(uris[record.uri].parentUri),
			authorDid: uriToDid(uris[record.uri].embedUri),
		});
		acc.push({
			parentUri: uris[record.uri].parentUri,
			embedUri: uris[record.uri].embedUri,
			violatesEmbeddingRules: !canEmbed,
		});
		return acc;
	}, [] as Array<{ parentUri: string; embedUri: string; violatesEmbeddingRules: boolean }>);
}

async function getReplyRefs(db: DatabaseSchema, reply: ReplyRef) {
	const replyRoot = reply.root.uri;
	const replyParent = reply.parent.uri;
	const replyGate = postUriToThreadgateUri(replyRoot);
	const results = await db.selectFrom("record").where("record.uri", "in", [
		replyRoot,
		replyGate,
		replyParent,
	]).leftJoin("post", "post.uri", "record.uri").selectAll("post").select(["record.uri", "json"])
		.execute();
	const root = results.find((ref) => ref.uri === replyRoot);
	const parent = results.find((ref) => ref.uri === replyParent);
	const gate = results.find((ref) => ref.uri === replyGate);
	return {
		root: root
			&& {
				uri: root.uri,
				invalidReplyRoot: root.invalidReplyRoot,
				record: jsonStringToLex(root.json) as PostRecord,
			},
		parent: parent
			&& {
				uri: parent.uri,
				invalidReplyRoot: parent.invalidReplyRoot,
				record: jsonStringToLex(parent.json) as PostRecord,
			},
		gate: gate && { uri: gate.uri, record: jsonStringToLex(gate.json) as GateRecord },
	};
}

async function fastRowCount(db: DatabaseSchema, table: string) {
	return sql<{ row_count: number }>`
		SELECT ((reltuples::bigint / relpages::bigint)::bigint * (pg_relation_size(oid) / current_setting('block_size')::int))::bigint
		AS row_count FROM pg_class
		WHERE oid = ${sql.literal(table)}::regclass
	`.execute(db).then((res) => res.rows[0].row_count);
}

async function alterDbSettings(db: Database) {
	// return Promise.all(
	// 	Object.entries(DB_SETTINGS).map(([setting, value]) =>
	// 		db.pool.query(`ALTER SYSTEM SET ${setting} = ${value}`)
	// 	),
	// );
}

function addExitHandlers(db: Database) {
	let reset = false;
	process.on("beforeExit", async () => {
		console.log("Resetting DB settings");
		// await Promise.all(
		// 	Object.keys(DB_SETTINGS).map((setting) =>
		// 		db.pool.query(`ALTER SYSTEM RESET ${setting}`)
		// 	),
		// );

		console.log("Closing DB connection");
		await db.pool.end();
		reset = true;
	});
	process.on("exit", (code) => {
		if (reset) return;
		// console.log(
		// 	Object.keys(DB_SETTINGS).map((setting) => `ALTER SYSTEM RESET ${setting};`).join(" "),
		// );
		console.log(`Exiting with code ${code}`);
	});
}

interface State {
	postCursor: string | null;
	profileCursor: string | null;
	validationIndex: number;
}

function loadState(): State {
	if (!fs.existsSync(statePath)) {
		state = { postCursor: null, profileCursor: null, validationIndex: 0 };
		saveState((s) => s);
		return state;
	}
	return state = JSON.parse(fs.readFileSync(statePath, "utf-8"));
}

function saveState(updateState: (state: State) => State) {
	fs.writeFileSync(statePath, JSON.stringify(updateState(state), null, 2));
}
