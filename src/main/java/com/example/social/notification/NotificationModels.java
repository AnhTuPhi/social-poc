package com.example.social.notification;

import java.time.Instant;
import java.time.LocalTime;
import java.util.List;

public class NotificationModels {

    public enum Channel { PUSH, EMAIL, IN_APP }
    public enum Category { LIKE, COMMENT, FOLLOW, MENTION, SYSTEM }
    public enum DeliveryStatus { PENDING, BATCHED, SENT, SUPPRESSED_QUIET_HOURS, DEDUPED }

    public record IncomingEvent(
        String userId,
        Category category,
        String actorId,
        String actorName,
        String targetId,
        String body
    ) {}

    public record UserPrefs(
        String userId,
        boolean pushEnabled,
        boolean emailEnabled,
        boolean inAppEnabled,
        LocalTime quietStart,
        LocalTime quietEnd,
        long batchWindowMs
    ) {}

    public record Notification(
        long id,
        String userId,
        Category category,
        String dedupKey,
        String title,
        String body,
        List<String> actorNames,
        int aggregatedCount,
        Channel channel,
        DeliveryStatus status,
        Instant firstSeen,
        Instant deliveredAt
    ) {}

    public record EngineMetrics(
        long eventsReceived,
        long deduped,
        long batched,
        long sent,
        long suppressedQuiet,
        long pendingInBuffer,
        double dedupRate,
        double batchEfficiency
    ) {}
}
