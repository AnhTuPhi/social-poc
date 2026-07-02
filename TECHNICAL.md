# TECHNICAL.md — Solution shapes, tech by responsibility, and tech debt

This document explains, per POC: the **hard problem**, **what we protect**, the **solution
shape**, the **key tech mapped to the responsibility it serves**, **how each sub-problem is
solved**, and the **tech debt** we knowingly accept.

For the *why* behind each problem, see [ISSUE.md](ISSUE.md). For what changes when you run
more than one instance, see [CONSISTENCY.md](CONSISTENCY.md).

Common baseline for all three POCs:

- **Spring Boot 3.4.3 / Java 21**, single process, all state in-memory.
- **Virtual threads enabled** (`spring.threads.virtual.enabled=true`) so load tests can spin
  thousands of concurrent tasks cheaply.
- Each POC = one `@Service` (the algorithm) + one `@RestController` (HTTP surface) + one
  static HTML page (interactive demo + live metrics).
- Concurrency primitives: `ConcurrentHashMap`, `AtomicLong`, `ReentrantLock`, and
  `@Scheduled` background flushers.

---

## POC #23 — News Feed fan-out

Source: [`feed/`](src/main/java/com/example/social/feed/) · UI: [`/feed.html`](src/main/resources/static/feed.html)

### The hard problem
A single celebrity post (180M followers) must not translate into 180M synchronous writes,
**and** a normal user's feed read must not translate into a fan-in over everyone they follow.
Optimizing one end pessimizes the other.

### What we protect
Post-time latency for high-follower authors **and** read-time latency for ordinary users —
simultaneously.

### Solution shape
Route each post through one of three strategies and let a **per-author decision** pick the
right one:

- **PUSH** (fan-out-on-write): at post time, copy the post into every follower's precomputed
  feed. Cheap reads, expensive writes.
- **PULL** (fan-out-on-read): store the post once; at read time, gather + merge recent posts
  from every followed author. Cheap writes, expensive reads.
- **HYBRID** (the answer): **push for normal authors, pull for celebrities.** Normal authors
  fan out to their small follower set; celebrity posts are *not* fanned out and are instead
  merged in at read time only for users who follow them.

The celebrity boundary is a follower-count threshold: `CELEBRITY_THRESHOLD = 10_000`
([`FeedService.java:14`](src/main/java/com/example/social/feed/FeedService.java:14)).

### Key tech by responsibility

| Responsibility | Mechanism |
|---|---|
| Per-user precomputed timeline (push target) | `Map<Long, Deque<FeedItem>> userFeeds`, capped at `MAX_FEED_ITEMS = 50` |
| Follower / following graph | `Map<Long, Set<Long>> followers` / `following` (`ConcurrentHashMap.newKeySet()`) |
| Author's own posts (pull source) | `Map<Long, List<Post>> authorPosts` |
| Push fan-out | `fanOutPush()` iterates followers, `addFirst` + trim to 50 |
| Pull merge | `pullFromAllAuthors()` / hybrid merge sorts by `createdAt` desc, limits to 50 |
| Celebrity classification | `followerCount >= CELEBRITY_THRESHOLD` at user creation |
| Live cost visibility | `AtomicLong` counters: `pushWrites`, `pullMerges`, per-strategy latency |

### How each sub-problem is solved
- **Celebrity write amplification** → hybrid *skips* fan-out for celebrities
  ([`FeedService.java:106`](src/main/java/com/example/social/feed/FeedService.java:106)); the
  180M-write cost never happens. `/simulate` even reports the writes hybrid *avoided*.
- **Normal-user read fan-in** → normal authors are pushed, so a normal read is a single
  `Deque` copy, not a graph traversal.
- **Mixed feeds** (you follow both normal people and celebrities) → hybrid read takes the
  precomputed deque **and** merges in celebrity pulls, then sorts + trims to 50.
- **Unbounded feed growth** → every write path trims to `MAX_FEED_ITEMS`.

### Tech debt acknowledged
- **Fan-out is synchronous and in-request.** Real systems push fan-out onto a queue
  (Kafka) and a worker pool; here `post()` blocks until every follower is written.
- **The follower graph and all feeds are in one JVM heap.** No sharding, no persistence.
- **PULL re-reads the author's last 10 posts every time** — no read caching.
- **Threshold is a single global constant.** Real systems tune per-author and also consider
  active-follower ratio, not just raw count.
- **No ranking.** Feeds are strictly reverse-chronological; no relevance model.

---

## POC #24 — Notification engine

Source: [`notification/`](src/main/java/com/example/social/notification/) · UI: [`/notification.html`](src/main/resources/static/notification.html)

### The hard problem
50 likes in 5 minutes must become **one** notification, not 50 — while still respecting quiet
hours, multiple channels with per-user preferences, and collapsing true duplicates.

### What we protect
User attention (delivery count) and push-gateway cost, without dropping information the user
wants.

### Solution shape
**Buffer → dedup → time-window batch → deliver.** Events are never delivered on ingest. Each
event is reduced to a **dedup key** and merged into a per-user, per-key `PendingBatch`. A
`@Scheduled` flusher runs every second and delivers only batches whose age has exceeded the
user's batch window, fanning each out across enabled channels with quiet-hours rules applied.

### Key tech by responsibility

| Responsibility | Mechanism |
|---|---|
| Collapse "same thing, many times" | **dedup key** built per category — `LIKE/COMMENT/MENTION` key on `targetId`, `FOLLOW` on `:self`, `SYSTEM` always unique (`buildDedupKey`) |
| Hold events without delivering | two-level buffer `Map<userId, Map<dedupKey, PendingBatch>>` |
| Merge duplicates into an aggregate | `PendingBatch.merge()` bumps `count`, unions `actorNames` (`LinkedHashSet`) |
| Fire batches when "ripe" | `@Scheduled(fixedDelay=1000)` `flushReadyBatches()`, window per-user (`batchWindowMs`) |
| Human-readable aggregation | `buildTitle` / `buildBody` — "Alice and 49 others liked your post" |
| Quiet hours | `inQuietHours()` handles same-day and overnight windows; suppresses **PUSH only** |
| Multi-channel with per-user prefs | `Channel` enum × `UserPrefs` booleans in `deliver()` |
| Effectiveness visibility | dedup rate + batch efficiency (`events / batches`) in `EngineMetrics` |

### How each sub-problem is solved
- **Notification storm** → 50 like events share one dedup key → one `PendingBatch` →
  one delivery per channel. `/burst` demonstrates 50 → 1 live.
- **True duplicates** → same key merges instead of appending; unlike/relike doesn't multiply.
- **Timing** → nothing delivers until `firstSeen` is older than the window, so late-arriving
  events in the window still join the same batch.
- **Do-not-disturb** → PUSH during quiet hours becomes `SUPPRESSED_QUIET_HOURS`; email/in-app
  still go through (a deliberate policy choice, tunable per channel).
- **Per-user control** → `PUT /prefs` changes channels, quiet window, and batch window live.

### Tech debt acknowledged
- **Suppressed-during-quiet notifications are dropped, not deferred.** A real system would
  re-queue them for the end of the quiet window.
- **The flusher scans the entire buffer every second** — O(users × keys). Fine for a demo,
  not for millions of users; production uses a time-bucketed / delay queue.
- **Quiet hours use the server's `ZoneId.systemDefault()`**, not the user's timezone.
- **`SYSTEM` events are intentionally never deduped** (random UUID key) — correct, but means
  a buggy system-event producer could flood a user.
- **No durability / retries / delivery receipts.** "Sent" means "appended to a list."

---

## POC #25 — Sharded counter

Source: [`counter/`](src/main/java/com/example/social/counter/) · UI: [`/counter.html`](src/main/resources/static/counter.html)

### The hard problem
Thousands of concurrent increments to one hot key (a viral like counter) serialize behind a
single lock/row. Throughput collapses to single-writer speed even on an idle box.

### What we protect
Write throughput and latency on a single hot key, while keeping the total correct (or
acceptably eventually-correct).

### Solution shape
Three strategies, run side by side and load-tested with virtual threads:

- **NAIVE_ROW_LOCK** — one `ReentrantLock` per key + simulated ~5µs "DB IO" under the lock.
  This is the failure mode: it *measures its own contention*.
- **SHARDED** — spread each key across **32 `AtomicLong` shards**; each increment hits a
  random shard (no cross-shard contention); reads **sum all shards**. Trades read cost for
  write scalability.
- **REDIS_FLUSH** — a single in-memory *hot* counter absorbs increments at `AtomicLong` speed;
  a `@Scheduled` job flushes the hot value to a "persisted" store every second. Models
  Redis `INCR` + async write-behind to a database.

### Key tech by responsibility

| Responsibility | Mechanism |
|---|---|
| Serialize (the anti-pattern) | `Map<String, ReentrantLock>` + `busyMicros(5)` under lock |
| Detect contention | check `lock.isLocked()` before acquiring → `contended` flag + counters |
| Scale writes | `Map<String, AtomicLong[SHARD_COUNT=32]>`, `ThreadLocalRandom` shard pick |
| Correct read from shards | `sumShards()` folds 32 cells into one total |
| Absorb writes hot | `Map<String, AtomicLong> hotCounters` (single-cell, lock-free `addAndGet`) |
| Durable-ish total | `Map<String, AtomicLong> persistedValues` + `@Scheduled(1000)` `flushHotToPersistent()` |
| Read-your-writes for hot path | `read()` returns `max(hot, persisted)` — hot is always ≥ persisted |
| Prove it under load | `loadTest()` on `Executors.newVirtualThreadPerTaskExecutor()`, `CountDownLatch` start gate |

### How each sub-problem is solved
- **Hot-key contention** → SHARDED removes the single contention point: N random shards mean
  writers rarely collide. REDIS_FLUSH removes locking entirely (`AtomicLong.addAndGet`).
- **Keeping the total correct** → shard reads sum all cells; the load test asserts
  `actual >= expected` to confirm no lost updates.
- **Durability vs speed** → REDIS_FLUSH decouples the write rate (in-memory) from the
  persistence rate (once/sec), the classic write-behind trade.
- **Making the problem visible** → NAIVE strategy counts its own `lock.isLocked()` hits so the
  UI can show contention climbing with thread count.

### Tech debt acknowledged
- **All three "stores" are JVM heap.** `hotCounters`/`persistedValues`/shards are `AtomicLong`,
  not Redis or a DB — so in a single process they're trivially consistent, which *hides* the
  real cross-instance problem (see [CONSISTENCY.md](CONSISTENCY.md)).
- **`busyMicros` is a spin-wait**, not real IO — it *understates* naive's true cost (a real
  row lock held across network+disk IO is far worse).
- **REDIS_FLUSH loses up to ~1s of increments on crash** (the un-flushed hot delta). That's
  the accepted durability window of write-behind — real systems make Redis itself durable.
- **`newValue` is `-1` for SHARDED increments** (shard-local; you must `GET /value` to get the
  sum) — an API wart that reflects the "read is the expensive side now" trade.
- **32 shards is a fixed constant.** Real systems size shards to peak concurrency and may
  adapt hot vs cold keys differently.

---

## Cross-cutting summary

| Concern | #23 Feed | #24 Notify | #25 Counter |
|---|---|---|---|
| Core move | push vs pull per author | buffer + dedup + batch | shard / absorb, reconcile later |
| Hot entity | celebrity | popular post | viral counter key |
| Trade made | write cost ↔ read cost | latency ↔ delivery volume | write speed ↔ read cost / durability |
| Background worker | (none; synchronous) | `@Scheduled` flusher (1s) | `@Scheduled` flush (1s) |
| Correctness model | eventually-merged at read | at-least-batched | sum / eventually-persisted |
| Biggest debt | synchronous in-request fan-out | in-memory per-instance buffer | in-memory "Redis"/"DB" |

Every "biggest debt" row is the **same debt**: state lives in one JVM. That is exactly why it
runs and reads cleanly — and exactly what has to change to scale horizontally.
