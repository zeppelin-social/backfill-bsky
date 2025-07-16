import {
	AppBskyActorProfile,
	AppBskyActorStatus,
	AppBskyFeedGenerator,
	AppBskyFeedLike,
	AppBskyFeedPost,
	AppBskyFeedPostgate,
	AppBskyFeedRepost,
	AppBskyFeedThreadgate,
	AppBskyGraphBlock,
	AppBskyGraphFollow,
	AppBskyGraphList,
	AppBskyGraphListblock,
	AppBskyGraphListitem,
	AppBskyGraphStarterpack,
	AppBskyGraphVerification,
	AppBskyLabelerService,
	AppBskyNotificationDeclaration,
	ChatBskyActorDeclaration,
} from "@atcute/bluesky";
import { type InferOutput, is as lexIs } from "@atcute/lexicons";

export const lexicons = {
	"app.bsky.actor.profile": AppBskyActorProfile.mainSchema,
	"app.bsky.actor.status": AppBskyActorStatus.mainSchema,
	"app.bsky.feed.generator": AppBskyFeedGenerator.mainSchema,
	"app.bsky.feed.like": AppBskyFeedLike.mainSchema,
	"app.bsky.feed.post": AppBskyFeedPost.mainSchema,
	"app.bsky.feed.postgate": AppBskyFeedPostgate.mainSchema,
	"app.bsky.feed.repost": AppBskyFeedRepost.mainSchema,
	"app.bsky.feed.threadgate": AppBskyFeedThreadgate.mainSchema,
	"app.bsky.graph.block": AppBskyGraphBlock.mainSchema,
	"app.bsky.graph.follow": AppBskyGraphFollow.mainSchema,
	"app.bsky.graph.list": AppBskyGraphList.mainSchema,
	"app.bsky.graph.listblock": AppBskyGraphListblock.mainSchema,
	"app.bsky.graph.listitem": AppBskyGraphListitem.mainSchema,
	"app.bsky.graph.starterpack": AppBskyGraphStarterpack.mainSchema,
	"app.bsky.graph.verification": AppBskyGraphVerification.mainSchema,
	"app.bsky.labeler.service": AppBskyLabelerService.mainSchema,
	"chat.bsky.actor.declaration": ChatBskyActorDeclaration.mainSchema,
	"app.bsky.notification.declaration": AppBskyNotificationDeclaration.mainSchema,
};

export function is<T extends keyof typeof lexicons>(
	type: T,
	value: unknown,
): value is InferOutput<typeof lexicons[T]>;
export function is(type: string, value: unknown): value is unknown;
export function is(type: string, value: unknown): boolean {
	if (!(type in lexicons)) return false;
	return lexIs(lexicons[type as keyof typeof lexicons], value);
}
