package com.example.social.feed;

import java.time.Instant;
import java.util.List;

public class FeedModels {
    public enum Strategy { PUSH, PULL, HYBRID }

    public record User(long id, String name, int followerCount, boolean celebrity) {}

    public record Post(long id, long authorId, String authorName, String text, Instant createdAt) {}

    public record FeedItem(long postId, long authorId, String authorName, String text, Instant createdAt, String source) {}

    public record PostRequest(long authorId, String text) {}

    public record PostResponse(long postId, Strategy strategyUsed, int fanoutWrites, long latencyMicros) {}

    public record FeedResponse(long userId, Strategy strategyUsed, List<FeedItem> items, int merges, long latencyMicros) {}

    public record Metrics(
        long totalPosts,
        long totalFeedReads,
        long pushWrites,
        long pullMerges,
        long celebrityPosts,
        long normalPosts,
        double avgPushLatencyMs,
        double avgPullLatencyMs,
        double avgHybridLatencyMs,
        int celebrityThreshold
    ) {}
}
