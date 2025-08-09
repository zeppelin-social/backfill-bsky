import LargeSet from "large-set";
import { Client as OpenSearchClient } from "@opensearch-project/opensearch";
import Helpers from "@opensearch-project/opensearch/lib/Helpers.js";
import { POST_INDEX, type PostDoc, PROFILE_INDEX, type ProfileDoc } from "../backfill/workers/opensearch";

class CustomHelpers extends Helpers {
	constructor(opts: { client: OpenSearchClient; maxRetries: number }) {
		// @ts-expect-error
		super(opts);
	}

	async *scrollDocumentsWithId<TDoc = unknown>(...[params, options]: Parameters<Helpers["scrollDocuments"]>): AsyncIterable<TDoc & { _id: string }> {
    appendFilterPath('hits.hits._source,hits.hits._id', params, true);

    for await (const { body } of this.scrollSearch(params, options)) {
      const hits = body && body.hits && body.hits.hits ? body.hits.hits : [];
      for (const hit of hits) {
        yield { _id: hit._id, ...hit._source };
      }
    }
  }
}

function appendFilterPath(filter: string, params: { filter_path?: string | string[] }, force?: boolean) {
  if (params.filter_path !== undefined) {
    params.filter_path += ',' + filter;
  } else if (force === true) {
    params.filter_path = filter;
  }
}


const client = new OpenSearchClient({
	node: process.env.OPENSEARCH_URL,
	auth: {
		username: process.env.OPENSEARCH_USERNAME,
		password: process.env.OPENSEARCH_PASSWORD,
	},
});
const helpers = new CustomHelpers({ client, maxRetries: 3 })

const seenPosts = new LargeSet<string>();
const seenProfiles = new LargeSet<string>();

async function scanPosts() {
	for await (const document of helpers.scrollDocumentsWithId<PostDoc>({ index: POST_INDEX })) {
		const { did, record_rkey, _id: id } = document;
		const key = `${did}/${record_rkey}`;
		if (seenPosts.has(key)) {
			await client.delete({ index: POST_INDEX, id });
		} else {
			seenPosts.add(key);
		}
	}
}

async function scanProfiles() {
	for await (const document of helpers.scrollDocumentsWithId<ProfileDoc>({ index: PROFILE_INDEX })) {
		const { did,  _id: id } = document;
		const key = did;
		if (seenProfiles.has(key)) {
			await client.delete({ index: PROFILE_INDEX, id });
		} else {
			seenProfiles.add(key);
		}
	}
}

const results = await Promise.allSettled([scanPosts(), scanProfiles()]);
for (const result of results) {
	if (result.status === "rejected") {
		console.error("rejected", result.reason)
	} else {
		console.log("fulfilled");
	}
}
