package com.example.social.notification;

import com.example.social.notification.NotificationModels.*;
import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class NotificationEngine {

    private static final long DEFAULT_BATCH_WINDOW_MS = 5_000;

    private final Map<String, UserPrefs> prefs = new ConcurrentHashMap<>();
    private final Map<String, Map<String, PendingBatch>> buffer = new ConcurrentHashMap<>();
    private final List<Notification> delivered = Collections.synchronizedList(new ArrayList<>());

    private final AtomicLong idSeq = new AtomicLong(0);
    private final AtomicLong eventsReceived = new AtomicLong(0);
    private final AtomicLong dedupedCount = new AtomicLong(0);
    private final AtomicLong batchedCount = new AtomicLong(0);
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong suppressedCount = new AtomicLong(0);

    @PostConstruct
    public void seed() {
        prefs.put("alice", new UserPrefs("alice", true, true, true,
            LocalTime.of(22, 0), LocalTime.of(7, 0), DEFAULT_BATCH_WINDOW_MS));
        prefs.put("bob", new UserPrefs("bob", true, false, true,
            LocalTime.of(23, 0), LocalTime.of(6, 0), 3_000));
        prefs.put("carol", new UserPrefs("carol", false, true, true,
            LocalTime.of(0, 0), LocalTime.of(0, 0), 10_000)); // no quiet
    }

    public void ingest(IncomingEvent event) {
        eventsReceived.incrementAndGet();

        UserPrefs userPrefs = prefs.computeIfAbsent(event.userId(),
            uid -> new UserPrefs(uid, true, true, true,
                LocalTime.of(22, 0), LocalTime.of(7, 0), DEFAULT_BATCH_WINDOW_MS));

        String dedupKey = buildDedupKey(event);

        Map<String, PendingBatch> userBuffer = buffer.computeIfAbsent(
            event.userId(), k -> new ConcurrentHashMap<>());

        userBuffer.compute(dedupKey, (k, existing) -> {
            if (existing == null) {
                PendingBatch nb = new PendingBatch(event, dedupKey, Instant.now());
                batchedCount.incrementAndGet();
                return nb;
            } else {
                existing.merge(event);
                dedupedCount.incrementAndGet();
                return existing;
            }
        });
    }

    private String buildDedupKey(IncomingEvent ev) {
        return switch (ev.category()) {
            case LIKE, COMMENT -> ev.category() + ":" + ev.targetId();
            case FOLLOW -> ev.category() + ":self";
            case MENTION -> ev.category() + ":" + ev.targetId();
            case SYSTEM -> ev.category() + ":" + UUID.randomUUID();
        };
    }

    @Scheduled(fixedDelay = 1000)
    public void flushReadyBatches() {
        Instant now = Instant.now();
        for (var userEntry : buffer.entrySet()) {
            String userId = userEntry.getKey();
            UserPrefs up = prefs.get(userId);
            long window = up == null ? DEFAULT_BATCH_WINDOW_MS : up.batchWindowMs();
            var userBuffer = userEntry.getValue();

            List<String> readyKeys = userBuffer.entrySet().stream()
                .filter(e -> Duration.between(e.getValue().firstSeen(), now).toMillis() >= window)
                .map(Map.Entry::getKey)
                .toList();

            for (String key : readyKeys) {
                PendingBatch batch = userBuffer.remove(key);
                if (batch != null) deliver(batch, up);
            }
        }
    }

    private void deliver(PendingBatch batch, UserPrefs up) {
        boolean quiet = inQuietHours(up);
        for (Channel ch : Channel.values()) {
            if (!channelEnabled(ch, up)) continue;
            if (quiet && ch == Channel.PUSH) {
                Notification n = build(batch, ch, DeliveryStatus.SUPPRESSED_QUIET_HOURS, null);
                delivered.add(n);
                suppressedCount.incrementAndGet();
            } else {
                Notification n = build(batch, ch, DeliveryStatus.SENT, Instant.now());
                delivered.add(n);
                sentCount.incrementAndGet();
            }
        }
    }

    private boolean channelEnabled(Channel ch, UserPrefs up) {
        return switch (ch) {
            case PUSH -> up.pushEnabled();
            case EMAIL -> up.emailEnabled();
            case IN_APP -> up.inAppEnabled();
        };
    }

    private boolean inQuietHours(UserPrefs up) {
        if (up.quietStart().equals(up.quietEnd())) return false;
        LocalTime now = LocalTime.now(ZoneId.systemDefault());
        LocalTime start = up.quietStart(), end = up.quietEnd();
        if (start.isBefore(end)) {
            return !now.isBefore(start) && now.isBefore(end);
        } else {
            return !now.isBefore(start) || now.isBefore(end);
        }
    }

    private Notification build(PendingBatch batch, Channel ch, DeliveryStatus status, Instant when) {
        String title = buildTitle(batch);
        String body = buildBody(batch);
        return new Notification(
            idSeq.incrementAndGet(),
            batch.first().userId(),
            batch.first().category(),
            batch.dedupKey(),
            title,
            body,
            new ArrayList<>(batch.actorNames()),
            batch.count(),
            ch,
            status,
            batch.firstSeen(),
            when
        );
    }

    private String buildTitle(PendingBatch b) {
        IncomingEvent first = b.first();
        int n = b.count();
        return switch (first.category()) {
            case LIKE -> n == 1
                ? first.actorName() + " liked your post"
                : first.actorName() + " and " + (n - 1) + " others liked your post";
            case COMMENT -> n == 1
                ? first.actorName() + " commented"
                : n + " new comments on your post";
            case FOLLOW -> n == 1
                ? first.actorName() + " followed you"
                : first.actorName() + " and " + (n - 1) + " others followed you";
            case MENTION -> first.actorName() + " mentioned you";
            case SYSTEM -> first.body();
        };
    }

    private String buildBody(PendingBatch b) {
        if (b.count() <= 1) return b.first().body();
        return "Aggregated " + b.count() + " events: " +
            String.join(", ", b.actorNames().stream().limit(3).toList()) +
            (b.actorNames().size() > 3 ? "..." : "");
    }

    public List<Notification> recentDelivered(int limit) {
        synchronized (delivered) {
            int from = Math.max(0, delivered.size() - limit);
            List<Notification> sub = new ArrayList<>(delivered.subList(from, delivered.size()));
            Collections.reverse(sub);
            return sub;
        }
    }

    public List<PendingBatch> currentBuffer() {
        return buffer.values().stream()
            .flatMap(m -> m.values().stream())
            .sorted(Comparator.comparing(PendingBatch::firstSeen).reversed())
            .toList();
    }

    public EngineMetrics metrics() {
        long events = eventsReceived.get();
        long dedup = dedupedCount.get();
        long batches = batchedCount.get();
        double dedupRate = events == 0 ? 0 : (double) dedup / events;
        double batchEff = batches == 0 ? 0 : (double) (events) / batches;
        long pending = buffer.values().stream().mapToLong(Map::size).sum();
        return new EngineMetrics(events, dedup, batches, sentCount.get(), suppressedCount.get(),
            pending, dedupRate, batchEff);
    }

    public Collection<UserPrefs> allPrefs() {
        return prefs.values();
    }

    public void updatePrefs(UserPrefs p) {
        prefs.put(p.userId(), p);
    }

    public void reset() {
        buffer.clear();
        delivered.clear();
        idSeq.set(0);
        eventsReceived.set(0);
        dedupedCount.set(0);
        batchedCount.set(0);
        sentCount.set(0);
        suppressedCount.set(0);
        prefs.clear();
        seed();
    }

    public static final class PendingBatch {
        private final IncomingEvent first;
        private final String dedupKey;
        private final Instant firstSeen;
        private int count = 1;
        private final LinkedHashSet<String> actorNames = new LinkedHashSet<>();

        PendingBatch(IncomingEvent first, String dedupKey, Instant firstSeen) {
            this.first = first;
            this.dedupKey = dedupKey;
            this.firstSeen = firstSeen;
            this.actorNames.add(first.actorName());
        }

        synchronized void merge(IncomingEvent ev) {
            count++;
            actorNames.add(ev.actorName());
        }

        public IncomingEvent first() { return first; }
        public String dedupKey() { return dedupKey; }
        public Instant firstSeen() { return firstSeen; }
        public synchronized int count() { return count; }
        public synchronized Set<String> actorNames() { return new LinkedHashSet<>(actorNames); }
    }
}
