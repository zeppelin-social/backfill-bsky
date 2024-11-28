import { IdResolver } from "@atproto/identity";
import { BlockMap } from "@atproto/repo";
import {
	type AccountEvt,
	type Create,
	type Delete,
	type Event,
	Firehose,
	type IdentityEvt,
	MemoryRunner,
	type Update,
} from "@atproto/sync";
import { AtUri } from "@atproto/syntax";
import { CID } from "multiformats/cid";
import * as fs from "node:fs";

declare global {
	namespace NodeJS {
		interface ProcessEnv {
			BSKY_REPO_PROVIDER: string;
		}
	}
}

type SerializableCreate = Omit<Create, "uri" | "blocks" | "commit" | "cid"> & {
	uri: string;
	commit: string;
	cid: string;
};

type SerializableUpdate = Omit<Update, "uri" | "blocks" | "commit" | "cid"> & {
	uri: string;
	commit: string;
	cid: string;
};

type SerializableDelete = Omit<Delete, "uri" | "blocks" | "commit" | "cid"> & {
	uri: string;
	commit: string;
};

type SerializableCommitEvt = SerializableCreate | SerializableUpdate | SerializableDelete;

type SerializableEvent = SerializableCommitEvt | IdentityEvt | AccountEvt;

for (const envVar of ["BSKY_REPO_PROVIDER"]) {
	if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

function main() {
	const ws = fs.createWriteStream("relay-buffer.jsonl");

	const idResolver = new IdResolver();
	const runner = new MemoryRunner({ startCursor: 0 });
	const firehose = new Firehose({
		idResolver,
		service: process.env.BSKY_REPO_PROVIDER,
		runner,
		unauthenticatedCommits: true,
		unauthenticatedHandles: true,
		handleEvent: async (event) => {
			const ser = serializeEvent(event);
			ws.write(JSON.stringify(ser) + "\n");
		},
		onError: (err) => {
			console.error(err);
		},
	});

	firehose.start();

	process.on("SIGINT", () => ws.close());
	process.on("SIGTERM", () => ws.close());
	process.on("exit", () => ws.close());
}

function serializeEvent(event: Event): SerializableEvent {
	switch (event.event) {
		case "create":
		case "update":
			return {
				...event,
				uri: event.uri.toString(),
				commit: event.commit.toString(),
				cid: event.cid.toString(),
			};
		case "delete":
			return { ...event, uri: event.uri.toString(), commit: event.commit.toString() };
		case "account":
		case "identity":
			return event;
		default:
			throw new Error("Event not serializable", event);
	}
}

export function deserializeEvent(event: SerializableEvent): Event {
	switch (event.event) {
		case "create":
		case "update":
			return {
				...event,
				uri: new AtUri(event.uri),
				commit: CID.parse(event.commit),
				cid: CID.parse(event.cid),
				blocks: new BlockMap(),
			};
		case "delete":
			return {
				...event,
				uri: new AtUri(event.uri),
				commit: CID.parse(event.commit),
				blocks: new BlockMap(),
			};
		case "account":
		case "identity":
			return event;
		default:
			throw new Error("Event not deserializable", event);
	}
}

main();
