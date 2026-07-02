# ISSUE — Social System Design POCs

## Context

Every consumer-social product eventually hits the same three scaling walls. They look
unrelated on the surface (a timeline, a notification tray, a like counter) but they are
all the **same underlying problem**: a small number of *hot* entities generate a
disproportionate amount of work, and the naive read/write path collapses under it.

This repository is a set of **three runnable proof-of-concepts** that isolate each wall,
demonstrate the naive failure mode, and show the industry-standard mitigation — all in one
Spring Boot 3.4 / Java 21 app with a browser UI per POC.

The POCs are intentionally **in-memory and single-process** so the *algorithm* is visible
without infrastructure noise. The consequences of that simplification — and what has to
change when you run more than one instance — are the subject of [CONSISTENCY.md](CONSISTENCY.md).

---

## The three issues

### Issue #23 — News Feed fan-out: the celebrity write-amplification problem

**Symptom.** When a user with 180,000,000 followers posts once, a naive "write to every
follower's timeline" (fan-out-on-write / *push*) turns **one** write into **180 million**
writes. A handful of celebrities can saturate the entire write path and stall posting for
everyone.

**The opposite failure.** If you instead compute every timeline at read time
(fan-out-on-read / *pull*), a normal user's feed read now has to query and merge posts from
everyone they follow — turning a cheap timeline load into a fan-in over hundreds of authors,
on *every* refresh, for *every* user.

**The tension.** Push is cheap to read, ruinous to write for celebrities. Pull is cheap to
write, ruinous to read for everyone. Neither works at both ends.

**What we must protect:** post-time latency for high-follower accounts **and** feed-read
latency for ordinary users, at the same time.

---

### Issue #24 — Notification engine: the notification-storm problem

**Symptom.** A popular post attracts 50 likes in 5 minutes. Naively, that is 50 push
notifications — 50 buzzes on the user's phone for one post. Multiply across every post and
every user and you get notification fatigue, users disabling notifications entirely, and a
push-gateway bill that scales with raw event volume instead of with *useful* deliveries.

**Secondary requirements that make it hard.** Deliveries must respect **quiet hours** (no
2 a.m. push), route across **multiple channels** (push / email / in-app) with per-channel
user preferences, and **collapse duplicates** ("Alice liked, then unliked, then liked" is
still one notification).

**What we must protect:** the user's attention (delivery count) and the push infrastructure
cost — without dropping information the user actually wants.

---

### Issue #25 — Sharded counter: the hot-key write-contention problem

**Symptom.** A viral post's like counter is a single row. Under a like storm, thousands of
concurrent increments all contend on one lock (or one row), serializing every write behind
every other write. Throughput on that key collapses to what a single writer can sustain,
and tail latency explodes even though the machine is mostly idle.

**The tension.** The correct value requires all increments to converge on one number, but
"one number" is exactly the contention point. You cannot both serialize on a single cell and
scale writes.

**What we must protect:** write throughput and latency on a single hot key, while keeping the
readable total correct (or acceptably eventually-correct).

---

## Why one repo

All three are variations of **"decouple the hot path from the expensive path."**

| POC | Hot path | Expensive path | Decoupling move |
|-----|----------|----------------|-----------------|
| #23 Feed | posting / reading | fan-out to N followers | choose push vs pull *per author* (hybrid) |
| #24 Notify | event ingest | user-facing delivery | buffer + dedup + time-window batch before delivery |
| #25 Counter | increment | durable single total | spread writes across shards / absorb in memory, reconcile later |

The POCs let you **run the naive version and watch it fail**, then flip a strategy and watch
the metrics recover. That contrast is the whole point.

---

## Non-goals

- **Not** production-ready services. State is in-memory; a restart wipes everything.
- **Not** horizontally correct as written — see [CONSISTENCY.md](CONSISTENCY.md) for what
  breaks the moment you run two instances.
- **Not** a benchmark of Spring/JVM performance — the load numbers are *relative* comparisons
  between strategies on the same box, not absolute capacity claims.

See [TECHNICAL.md](TECHNICAL.md) for the solution shape, key tech by responsibility, and the
acknowledged tech debt in each POC.
