import { EncoderStream } from "cbor-x";
import { createWriteStream } from "node:fs";
import { fetchAllDids } from "../backfill/util/fetch.js";

const stream = new EncoderStream();
stream.pipe(createWriteStream("dids.cache"));

let drainPromise: Promise<void> | null = null;
const createDrainPromise = () => {
	return new Promise<void>((resolve) => {
		stream.once("drain", () => {
			drainPromise = null;
			resolve();
		});
	});
};

const count = await fetchAllDids(async (did, pds) => {
	if (drainPromise) await drainPromise;
	if (!stream.write([did, pds])) drainPromise ??= createDrainPromise();
});

stream.end();
stream.on("close", () => console.log(`Wrote ${count} DIDs to dids.cache`));
