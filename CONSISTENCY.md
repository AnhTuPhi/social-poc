# CONSISTENCY.md — What breaks when you scale out (k8s pods / VMs)

Every POC here is **single-process and in-memory on purpose** — the algorithm is the point,
not the plumbing. But that same simplification means the correctness you observe is an
*artifact of running one instance*. The moment you run two — `kubectl scale deploy
social-pocs --replicas=3`, or two VMs behind a load balancer — most of it silently breaks.

This document explains, per POC, **exactly what breaks**, **why**, and **what the real fix
is**. The recurring theme:

> **Shared mutable state that lives in one JVM heap cannot be replicated by copying the JVM.**
> Scaling the pod scales the *stateless* compute; the *state* has to move out of the heap into
> something all instances share (Redis, Kafka, a DB) — or be *partitioned* so exactly one
> instance owns each key.

There are two legitimate fixes and you must pick one per piece of state:

- **Externalize** — move the state to a shared store every pod reads/writes (Redis, DB, Kafka
  log). All pods stay symmetric and stateless.
- **Partition** — assign each key to exactly one owner pod (consistent hashing, Kafka
  partition, sticky routing). The state stays in-heap but is never duplicated.

---

## The generic failure when you add a second pod

A round-robin load balancer sends request 1 to pod A and request 2 to pod B. Because each pod
has its **own** `ConcurrentHashMap` / `AtomicLong`, they diverge immediately:

```
        ┌── pod A heap: userFeeds{...}, buffer{...}, hotCounters{...}
LB ────►┤
        └── pod B heap: userFeeds{...}, buffer{...}, hotCounters{...}   ← different data
```

- A write on A is invisible to B.
- A read on B misses data written on A → **read-your-writes fails**.
- Two `@Scheduled` flushers now run — one per pod — with no coordination → **double
  side-effects**.

That last point is the sharpest one and easy to miss: `@Scheduled` fires on **every replica**.
Two POCs here (#24 and #25) rely on a scheduled flusher, so scaling out doesn't just split the
data — it *duplicates the background job*.

---

## POC #23 — News Feed fan-out

### What breaks
- **`userFeeds`, `followers`, `following`, `authorPosts` are per-pod.** A post handled by pod A
  fans out only into A's copy of the feeds. A follower whose read lands on pod B sees nothing.
- **PUSH fan-out is worse than useless across pods** — it writes into the heap of whichever pod
  received the post; other pods never learn of the write.
- **PULL is inconsistent** — pod B can only pull from the `authorPosts` *it* happens to hold.

### Why
Fan-out is fundamentally about writing into *other users'* state. If that state is sharded by
"whichever pod caught the request," there is no coherent per-user timeline anywhere.

### Real fix — externalize the timelines, partition the fan-out work
- **Timelines → Redis** (a `LIST`/`ZSET` per user), or Cassandra (`(user_id, ts)` partition).
  Every pod reads/writes the same feed regardless of who serves the request.
- **Follower graph → a DB / graph store**, cached read-through.
- **Fan-out → asynchronous, via Kafka.** `post()` publishes one event; a pool of fan-out
  workers (partitioned by `authorId` or by target-shard) writes into follower timelines. This
  also removes the synchronous-in-request debt noted in [TECHNICAL.md](TECHNICAL.md).
- **Hybrid still applies, and matters more:** celebrity pull-at-read-time now means a read
  merges a Redis timeline with a *cached* celebrity-posts lookup — the same code shape, backed
  by shared stores.

### Consistency model after the fix
Eventually consistent. A post is visible after fan-out workers drain the Kafka partition
(typically sub-second). Feed reads are read-your-writes only for the *pull* portion; the
*push* portion lags by fan-out latency — acceptable and expected for timelines.

---

## POC #24 — Notification engine

This is the POC that breaks **most severely** on scale-out, because both the buffer *and* the
flusher are stateful.

### What breaks
1. **Dedup only works within one pod.** 50 like events for the same post, spread by the LB
   across 3 pods, become **3 separate `PendingBatch` objects** (one per pod) → the user gets
   ~3 notifications instead of 1. Dedup degrades toward `#pods`.
2. **The flusher runs on every pod.** Each pod independently flushes *its* buffer, so even
   correctly-deduped batches multiply by the number of pods that hold a piece.
3. **`UserPrefs` set via `PUT /prefs` on pod A** isn't seen by pods B/C — a user could be in
   quiet hours on one pod and not another.

### Why
Dedup and batching are *aggregation over a window*. Aggregation is only correct if all events
for a given key reach the **same aggregator**. Round-robin routing guarantees they don't.

### Real fix — partition by user, then optionally externalize
- **Partition ingestion by `userId`.** Put events on **Kafka keyed by `userId`**; every event
  for a user lands on the same partition, consumed by exactly one pod. Now that pod is the sole
  owner of that user's buffer and dedup is correct again — *in-heap, no shared store needed*.
  This is the cleanest fix and keeps the existing `PendingBatch` logic almost unchanged.
- **Alternatively, externalize the buffer to Redis** with atomic ops: the dedup key becomes a
  Redis key (`buffer:{userId}:{dedupKey}`), `merge` becomes `HINCRBY` + `SADD actorNames`, with
  a TTL as the batch window. Any pod can serve any request, but you pay a round-trip per event.
- **The flusher must not run everywhere.** Options:
  - With Kafka partitioning: each pod flushes **only the partitions it owns** — naturally
    single-owner, no coordination.
  - With a shared Redis buffer: use a **leader-elected** flusher (k8s `Lease` / ShedLock /
    Redisson lock) so exactly one pod flushes, or use Redis keyspace-expiry events.
- **`UserPrefs` → shared store** (DB or Redis), cached with invalidation.

### Consistency model after the fix
Per-user total ordering and correct dedup via Kafka partition ownership; batches fire
eventually (within the window). This is at-least-once at the channel level unless you add an
idempotency key on delivery — worth doing so a partition rebalance can't re-send a batch.

---

## POC #25 — Sharded counter

The irony: this POC is *named* for the technique that fixes it, but as written its "Redis" and
"DB" are `AtomicLong`s in one heap, so it too is single-process only.

### What breaks per strategy
- **NAIVE_ROW_LOCK** — the `ReentrantLock` is a **JVM lock**. Across pods there is *no* mutual
  exclusion at all. Each pod increments its own `naiveValues` cell → totals diverge and are all
  wrong. (A real DB row lock would work across pods but reintroduce the contention this POC
  exists to demonstrate.)
- **SHARDED** — shards are `AtomicLong[]` **per pod**. Pod A's 32 shards and pod B's 32 shards
  are unrelated; summing on one pod misses the other's writes. Correct sharding requires the
  shards to live in a **shared** store.
- **REDIS_FLUSH** — `hotCounters` is a per-pod `AtomicLong`, so each pod absorbs a *different*
  slice of increments; and **every pod's `@Scheduled` flush overwrites the persisted value with
  `.set(v)`** — a last-writer-wins clobber that loses the other pods' counts entirely.

### Why
A counter is a single logical value with a single source of truth. Any in-heap representation
makes that source of truth *per-pod*, which is a contradiction.

### Real fix — this is the one POC where "just use Redis" is literally the answer
- **REDIS_FLUSH → real Redis `INCR`/`INCRBY`.** Redis is single-threaded per key, so `INCR` is
  atomic *across all pods* and lock-free from the app's perspective — high throughput on a hot
  key without any pod-side coordination. Increments never touch a DB row directly.
- **Write-behind flush → one owner.** Persist Redis → DB from a **single** leader (ShedLock /
  k8s `Lease`), or better, don't have the app do it — let a dedicated worker or Redis
  persistence handle durability. The flusher must be **additive** (`persisted += delta` or
  read-then-write the authoritative Redis value), never a per-pod `.set()`.
- **SHARDED still has a place** *inside* Redis (or a DB) if a single key becomes hot enough to
  bottleneck even Redis: keep `key:shard:{0..N}` as Redis keys, `INCR` a random one, sum on
  read. The POC's shard logic maps directly onto Redis keys.
- **NAIVE_ROW_LOCK** stays as the cautionary tale — its whole purpose is to be the thing you
  *don't* do.

### Consistency model after the fix
Strong for the counter value itself (Redis `INCR` is linearizable per key). The DB copy is
eventually consistent, lagging by the flush interval — the deliberate write-behind window.
Read paths that need the exact live value read Redis; analytics reads tolerate the lagging DB.

---

## Cross-cutting checklist for scaling any of these

| Question | #23 Feed | #24 Notify | #25 Counter |
|---|---|---|---|
| Where does mutable state live today? | JVM heap maps | JVM heap buffer + prefs | JVM heap AtomicLongs |
| Does a 2nd pod duplicate a `@Scheduled` job? | n/a (none) | **yes — flusher** | **yes — flush** |
| Fix pattern | externalize (Redis/Cassandra) + async fan-out (Kafka) | **partition by userId (Kafka)** or Redis buffer + leader flush | **externalize to Redis `INCR`** + single-owner write-behind |
| Routing requirement | any pod (after externalize) | **sticky/partitioned by user** | any pod (Redis is the SoT) |
| Resulting consistency | eventual (fan-out lag) | per-user ordered, eventual batch | strong per key, eventual DB |

### The three rules this all reduces to
1. **State out of the heap, or one owner per key.** Never both replicate the process *and* keep
   authoritative state inside it.
2. **A `@Scheduled` job in a replicated deployment runs N times.** Guard every background job
   with partition-ownership or leader election.
3. **Match routing to the aggregation boundary.** If correctness requires all events for a key
   to meet, route them to meet (partition/sticky) — round-robin is only safe for stateless work.
