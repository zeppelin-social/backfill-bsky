import { PackrStream } from "msgpackr";
import fs from "node:fs";
import { fetchAllDids } from "./util/fetch";

const stream = new PackrStream();
stream.pipe(fs.createWriteStream("dids.cache"));

const count = await fetchAllDids((did, pds) => stream.write([did, pds]));
stream.end();

console.log(`Wrote ${count} DIDs to dids.cache`);
