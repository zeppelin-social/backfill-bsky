import { EncoderStream } from "cbor-x";
import { createWriteStream } from "node:fs";
import { fetchAllDids } from "../backfill/util/fetch.js";

const stream = new EncoderStream();
stream.pipe(createWriteStream("dids.cache"));

let waitingForDrain = false;

const count = await fetchAllDids(async (did, pds) => {
	if (waitingForDrain) await waitForStreamDrain(stream);
	if (!stream.write([did, pds])) {
		waitingForDrain = true;
	}
});
stream.end();
stream.on("close", () => console.log(`Wrote ${count} DIDs to dids.cache`));

async function waitForStreamDrain(stream: EncoderStream) {
	if (!waitingForDrain) return;
	return new Promise<void>((resolve) => {
		stream.on("drain", () => {
			waitingForDrain = false;
			resolve();
		});
	});
}
