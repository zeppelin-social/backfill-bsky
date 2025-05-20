import { PackrStream } from "msgpackr";
import { createWriteStream } from "node:fs";
import { fetchAllDids } from "../backfill/util/fetch.js";

const stream = new PackrStream();
stream.pipe(createWriteStream("dids.cache"));

const count = await fetchAllDids((did, pds) => stream.write([did, pds]));
stream.end();

console.log(`Wrote ${count} DIDs to dids.cache`);
