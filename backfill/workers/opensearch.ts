import type { AppBskyActorProfile, AppBskyFeedPost } from "@atcute/bluesky";
import { type Did, parseCanonicalResourceUri } from "@atcute/lexicons";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import fs from "node:fs/promises";
import normalizeUrl from "normalize-url";
import type { FromWorkerMessage } from "../main.js";
import type { CommitMessage } from "./repo.js";

const POST_INDEX = "palomar_post";
const PROFILE_INDEX = "palomar_profile";
const ES_INDEXES = [POST_INDEX, PROFILE_INDEX];

export async function openSearchWorker() {
	console.info(`Starting OpenSearch worker`);

	const client = new OpenSearchClient({
		node: process.env.OPENSEARCH_URL,
		auth: {
			username: process.env.OPENSEARCH_USERNAME,
			password: process.env.OPENSEARCH_PASSWORD,
		},
	});

	for (const index of ES_INDEXES) {
		if (!await client.indices.exists({ index })) {
			await client.indices.create({ index });
		}
	}

	let postQueue: Array<PostDoc> = [];
	let profileQueue: Array<ProfileDoc> = [];

	let queueTimer = setTimeout(processQueue, 2000);

	let isShuttingDown = false;

	process.on("message", async (msg: CommitMessage | { type: "shutdown" }) => {
		if (msg.type === "shutdown") {
			await handleShutdown();
			return;
		}

		if (isShuttingDown) return;

		if (msg.type !== "commit") {
			throw new Error(`Invalid message type ${msg}`);
		}

		if (
			msg.collection !== "app.bsky.feed.post" && msg.collection !== "app.bsky.actor.profile"
		) {
			return;
		}

		for (const commit of msg.commits) {
			const { did, path, cid, timestamp, obj } = commit;
			if (!did || !path || !cid || !timestamp || !obj) {
				console.error(`Invalid commit data ${JSON.stringify(commit)}`);
				continue;
			}

			if (msg.collection === "app.bsky.feed.post") {
				const rkey = path.split("/")[1];
				if (rkey) {
					postQueue.push(
						// We can assert obj because the repo worker checks for record validity
						transformPost(obj as AppBskyFeedPost.Main, did as Did, rkey, cid),
					);
				}
			} else if (msg.collection === "app.bsky.actor.profile") {
				profileQueue.push(
					// We can assert obj because the repo worker checks for record validity
					transformProfile(obj as AppBskyActorProfile.Main, did as Did, cid),
				);
			}
		}

		if (postQueue.length > 100_000 || profileQueue.length > 100_000) {
			clearTimeout(queueTimer);
			queueTimer = setImmediate(processQueue);
		}
	});

	process.on("uncaughtException", (err) => {
		console.error(`Uncaught exception in OpenSearch worker`, err);
	});

	process.on("SIGTERM", handleShutdown);
	process.on("SIGINT", handleShutdown);

	async function processQueue() {
		if (!isShuttingDown) {
			queueTimer = setTimeout(processQueue, 2000);
		}

		const time =
			`Writing ${postQueue.length} posts and ${profileQueue.length} profiles to OpenSearch`;

		const datasource = [...postQueue, ...profileQueue];
		postQueue = [];
		profileQueue = [];

		try {
			if (datasource.length > 0) {
				console.time(time);
				await client.helpers.bulk({
					datasource,
					onDocument: (doc) => {
						return {
							index: { _index: "record_rkey" in doc ? POST_INDEX : PROFILE_INDEX },
						};
					},
				});
				console.timeEnd(time);
			}
		} catch (err) {
			console.error(`Error processing OpenSearch queue`, err);
			await fs.writeFile(
				`./failed-search.jsonl`,
				datasource.map((r) => JSON.stringify(r)).join("\n") + "\n",
				{ flag: "a" },
			);
			console.timeEnd(time);
		}
	}

	async function handleShutdown() {
		console.log(`OpenSearch worker received shutdown signal`);
		isShuttingDown = true;
		await processQueue();
		process.send?.({ type: "shutdownComplete" } satisfies FromWorkerMessage);
		process.exit(0);
	}
}

// Everything below is ported nearly 1:1 from github.com/bluesky-social/indigo/tree/main/search/transform.go

function transformProfile(profile: AppBskyActorProfile.Main, did: Did, cid: string): ProfileDoc {
	const emojis = profile.description ? parseEmojis(profile.description) : undefined;
	const selfLabels = profile.labels?.values?.map((v) => v.val) ?? [];

	return {
		doc_index_ts: new Date().toISOString(),
		did,
		record_cid: cid,
		handle: "",
		display_name: profile.displayName,
		description: profile.description,
		self_label: selfLabels.length ? selfLabels : undefined,
		emoji: emojis?.length ? emojis : undefined,
		has_avatar: !!profile.avatar,
		has_banner: !!profile.banner,
	};
}

function transformPost(post: AppBskyFeedPost.Main, did: Did, rkey: string, cid: string): PostDoc {
	const has = new Set<string>();

	const altText: string[] = [];
	if (post.embed?.$type === "app.bsky.embed.images") {
		has.add("image");
		for (const img of post.embed.images) {
			if (img.alt) {
				altText.push(img.alt);
			}
		}
	}
	const langCodeIso2: string[] = [];
	for (const lang of post.langs || []) {
		// TODO: include an actual language code map to go from 3char to 2char
		const prefix = lang.split("-", 2)[0];
		if (prefix.length === 2) {
			langCodeIso2.push(prefix.toLowerCase());
		}
	}
	const mentionDIDs: string[] = [];
	const urls: string[] = [];
	for (const facet of post.facets || []) {
		for (const feat of facet.features) {
			if (feat.$type === "app.bsky.richtext.facet#mention") {
				has.add("mention");
				mentionDIDs.push(feat.did);
			}
			if (feat.$type === "app.bsky.richtext.facet#link") {
				has.add("link");
				urls.push(feat.uri);
			}
		}
	}
	let replyRootATURI: string | undefined;
	let parentDID: string | undefined;
	if (post.reply) {
		replyRootATURI = post.reply.root.uri;
		const parsedATURI = parseCanonicalResourceUri(post.reply.parent.uri);
		if (parsedATURI.ok) parentDID = parsedATURI.value.repo;
	}
	if (post.embed?.$type === "app.bsky.embed.external") {
		has.add("link");
		urls.push(post.embed.external.uri);
	}
	let embedATURI: string | undefined;
	if (post.embed?.$type === "app.bsky.embed.record") {
		has.add("quote");
		embedATURI = post.embed.record.uri;
	}
	if (post.embed?.$type === "app.bsky.embed.recordWithMedia") {
		has.add("quote");
		embedATURI = post.embed.record.record.uri;
	}
	let embedImgCount = 0;
	const embedImgAltText: string[] = [];
	const embedImgAltTextJA: string[] = [];
	if (post.embed?.$type === "app.bsky.embed.images") {
		has.add("image");
		embedImgCount = post.embed.images.length;
		for (const img of post.embed.images) {
			if (img.alt) {
				embedImgAltText.push(img.alt);
				if (japaneseRegex.test(img.alt)) {
					embedImgAltTextJA.push(img.alt);
				}
			}
		}
	}

	if (post.embed?.$type === "app.bsky.embed.video") {
		has.add("video");
		embedImgCount = 1;
		if (post.embed?.alt) {
			embedImgAltText.push(post.embed.alt);
			if (japaneseRegex.test(post.embed.alt)) {
				embedImgAltTextJA.push(post.embed.alt);
			}
		}
	}

	if (
		post.embed?.$type === "app.bsky.embed.external"
		&& post.embed.external.uri.startsWith("https://media.tenor.com")
	) {
		has.add("gif");
		embedImgCount = 1;
		const alt = post.embed.external.description;
		if (alt) {
			embedImgAltText.push(alt);
			if (japaneseRegex.test(alt)) {
				embedImgAltTextJA.push(alt);
			}
		}
	}

	if (post.embed?.$type === "app.bsky.embed.recordWithMedia") {
		if (
			post.embed.media.$type === "app.bsky.embed.images" && post.embed.media.images.length > 0
		) {
			has.add("image");
			embedImgCount += post.embed.media.images.length;
			for (const img of post.embed.media.images) {
				if (img.alt) {
					embedImgAltText.push(img.alt);
					if (japaneseRegex.test(img.alt)) {
						embedImgAltTextJA.push(img.alt);
					}
				}
			}
		} else if (post.embed.media.$type === "app.bsky.embed.video") {
			has.add("video");
			embedImgCount += 1;
			const alt = post.embed.media.alt;
			if (alt) {
				embedImgAltText.push(alt);
				if (japaneseRegex.test(alt)) {
					embedImgAltTextJA.push(alt);
				}
			}
		}
	}

	const selfLabels = post.labels?.values?.map((v) => v.val) ?? [];

	const domains: string[] = [];
	for (let i = 0; i < urls.length; i++) {
		try {
			const clean = normalizeLossyUrl(urls[i]);
			urls[i] = clean;
			const u = new URL(clean);
			domains.push(u.hostname);
		} catch (err) {
			// ignore error
		}
	}

	const doc: PostDoc = {
		doc_index_ts: new Date().toISOString(),
		did,
		record_rkey: rkey,
		record_cid: cid,
		text: post.text,
		lang_code: post.langs?.length ? post.langs : undefined,
		lang_code_iso2: langCodeIso2.length ? langCodeIso2 : undefined,
		mention_did: mentionDIDs.length ? mentionDIDs : undefined,
		parent_did: parentDID,
		embed_aturi: embedATURI,
		reply_root_aturi: replyRootATURI,
		embed_img_count: embedImgCount,
		embed_img_alt_text: embedImgAltText.length ? embedImgAltText : undefined,
		embed_img_alt_text_ja: embedImgAltTextJA.length ? embedImgAltTextJA : undefined,
		self_label: selfLabels.length ? selfLabels : undefined,
		url: urls.length ? urls : undefined,
		domain: domains.length ? domains : undefined,
		tag: parsePostTags(post),
		emoji: parseEmojis(post.text),
		has: has.size ? [...has] : undefined,
	};

	if (japaneseRegex.test(post.text)) {
		doc.text_ja = post.text;
	}

	if (post.createdAt) {
		// there are some old bad timestamps out there!
		try {
			const dt = new Date(post.createdAt);
			// not more than a few minutes in the future
			if (!isNaN(dt.getTime())) {
				doc.created_at = dt.toISOString();
			}
		} catch (err) {
			// ignore error
		}
	}

	return doc;
}

const trackingParams = [
	"__s",
	"_ga",
	"campaign_id",
	"ceid",
	"emci",
	"emdi",
	"fbclid",
	"gclid",
	"hootPostID",
	"mc_eid",
	"mkclid",
	"mkt_tok",
	"msclkid",
	"pk_campaign",
	"pk_kwd",
	"sessionid",
	"sourceid",
	"utm_campaign",
	"utm_content",
	"utm_id",
	"utm_medium",
	"utm_source",
	"utm_term",
	"xpid",
];

function normalizeLossyUrl(raw: string): string {
	let clean;
	try {
		clean = normalizeUrl(raw, {
			defaultProtocol: "https",
			stripHash: true,
			removeDirectoryIndex: true,
		});
	} catch {
		return raw;
	}
	let url;
	try {
		url = new URL(clean);
	} catch {
		return clean;
	}
	if (url.searchParams.size === 0) return clean;
	trackingParams.forEach((p) => url.searchParams.delete(p));
	return url.toString();
}

// U+3040 - U+30FF: hiragana and katakana (Japanese only)
// U+FF66 - U+FF9F: half-width katakana (Japanese only)
// will not trigger on CJK characters which are not specific to Japanese
const japaneseRegex = /[\u3040-\u30ff\uff66-\uff9f]/;

function parsePostTags(p: AppBskyFeedPost.Main): string[] | undefined {
	const ret: string[] = [];

	for (const facet of p.facets ?? []) {
		for (const feat of facet.features ?? []) {
			if (feat.$type === "app.bsky.richtext.facet#tag" && feat.tag) {
				ret.push(feat.tag);
			}
		}
	}

	if (p.tags) {
		ret.push(...p.tags);
	}

	if (ret.length === 0) return;
	return [...new Set(ret)];
}

function parseEmojis(s: string): string[] | undefined {
	const ret: string[] = [];
	const seen = new Set<string>();

	const segmenter = new Intl.Segmenter("en", { granularity: "grapheme" });
	const segments = segmenter.segment(s);

	for (const segment of segments) {
		const grapheme = segment.segment;
		const firstCodePoint = grapheme.codePointAt(0);

		if (firstCodePoint && isEmoji(firstCodePoint)) {
			if (!seen.has(grapheme)) {
				ret.push(grapheme);
				seen.add(grapheme);
			}
		}
	}

	if (ret.length === 0) return;
	return ret;
}

function isEmoji(codePoint: number): boolean {
	return (codePoint >= 0x1F000 && codePoint <= 0x1FFFF)
		|| (codePoint >= 0x2600 && codePoint <= 0x26FF);
}

type ProfileDoc = {
	doc_index_ts: string;
	did: string;
	record_cid: string;
	handle: string;
	display_name?: string;
	description?: string;
	img_alt_text?: string[];
	self_label?: string[];
	url?: string[];
	domain?: string[];
	tag?: string[];
	emoji?: string[];
	has_avatar: boolean;
	has_banner: boolean;
};

type PostDoc = {
	doc_index_ts: string;
	did: string;
	record_rkey: string;
	record_cid: string;
	created_at?: string;
	text: string;
	text_ja?: string;
	lang_code?: string[];
	lang_code_iso2?: string[];
	mention_did?: string[];
	parent_did?: string;
	embed_aturi?: string;
	reply_root_aturi?: string;
	embed_img_count: number;
	embed_img_alt_text?: string[];
	embed_img_alt_text_ja?: string[];
	self_label?: string[];
	url?: string[];
	domain?: string[];
	tag?: string[];
	emoji?: string[];
	has?: string[];
};
