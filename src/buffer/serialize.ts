import { BlockMap } from "@atproto/repo";
import type { AccountEvt, Create, Delete, Event, IdentityEvt, Update } from "@atproto/sync";
import { AtUri } from "@atproto/syntax";
import { CID } from "multiformats/cid";

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

export type SerializableEvent = SerializableCommitEvt | IdentityEvt | AccountEvt;

export function serializeEvent(event: Event): SerializableEvent {
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
			throw new Error("Event not serializable: " + JSON.stringify(event));
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
			throw new Error("Event not deserializable: " + JSON.stringify(event));
	}
}
