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

const POOL_SIZE = 500;

async function main() {
	const db = new Database({
		url: process.env.BSKY_DB_POSTGRES_URL,
		schema: process.env.BSKY_DB_POSTGRES_SCHEMA,
		poolSize: POOL_SIZE,
	});

	await backfillPostAggregates(db);
	await backfillProfileAggregates(db);
	await backfillPostValidation(db);
}

void main();

async function backfillPostAggregates({ db }: Database) {
	const limit = 10_000_000;

	const rowCount = await fastRowCount(db, "post");
	console.log(`post row count: ${rowCount}`);

	const batches = Math.ceil(rowCount / limit);
	let i = 0;
	try {
		for (; i < batches; i++) {
			const offset = i * limit;
			console.time(`backfilling posts ${i + 1}/${batches}`);
			await sql`
			WITH uris (uri) AS (
				SELECT uri FROM post WHERE uri IS NOT NULL LIMIT ${limit} OFFSET ${offset}
			)
			INSERT INTO post_agg ("uri", "replyCount", "likeCount", "repostCount")
			SELECT
				v.uri,
				count(post."replyParent") AS replyCount,
				count(like.uri) AS likeCount,
				count(repost.uri) AS repostCount
			FROM
				uris AS v
				LEFT JOIN post ON post."replyParent" = v.uri
					AND (post."violatesThreadGate" IS NULL OR post."violatesThreadGate" = false)
				LEFT JOIN like ON like.subject = v.uri
				LEFT JOIN repost ON repost.subject = v.uri
			GROUP BY v.uri
			ON CONFLICT (uri) DO UPDATE SET "replyCount" = excluded."replyCount", "likeCount" = excluded."likeCount", "repostCount" = excluded."repostCount"
		`.execute(db);
			console.timeEnd(`backfilling posts ${i + 1}/${batches}`);
		}
	} catch (err) {
		console.error(`backfilling posts ${i + 1}/${batches}`, err);
	}
}

async function backfillProfileAggregates({ db }: Database) {
	const limit = 1_000_000;

	const rowCount = await fastRowCount(db, "profile");
	console.log(`profile row count: ${rowCount}`);

	const batches = Math.ceil(rowCount / limit);
	let i = 0;
	try {
		for (; i < batches; i++) {
			const offset = i * limit;
			console.time(`backfilling profiles ${i + 1}/${batches}`);
			await sql`
			WITH dids (did) AS (
				SELECT did FROM profile WHERE did IS NOT NULL LIMIT ${limit} OFFSET ${offset}
			)
			INSERT INTO profile_agg ("did", "postsCount", "followersCount", "followsCount")
			SELECT
				v.did,
				count(post.creator) AS postsCount,
				count(followers."subjectDid") AS followersCount,
				count(follows.creator) AS followsCount
			FROM
				dids AS v
				LEFT JOIN post ON post.creator = v.did
				LEFT JOIN follow AS followers ON followers."subjectDid" = v.did
				LEFT JOIN follow AS follows ON follows.creator = v.did
			GROUP BY v.did
			ON CONFLICT (did) DO UPDATE SET "postsCount" = excluded."postsCount", "followersCount" = excluded."followersCount", "followsCount" = excluded."followsCount"
		`.execute(db);
			console.timeEnd(`backfilling profiles ${i + 1}/${batches}`);
		}
	} catch (err) {
		console.error(`backfilling profiles ${i + 1}/${batches}`, err);
	}
}

async function backfillPostValidation({ db }: Database) {
	const limit = 1_000_000;

	const rowCount = await fastRowCount(db, "post");
	console.log(`post row count: ${rowCount}`);

	const batches = Math.ceil(rowCount / limit);
	let i = 0;
	try {
		for (; i < batches; i++) {
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
			]).where("replyParent", "is not", null).where("replyRoot", "is not", null).limit(limit)
				.offset(offset).execute();

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
		}
	} catch (err) {
		console.error(`validating posts ${i + 1}/${batches}`, err);
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
