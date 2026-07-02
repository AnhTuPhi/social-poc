# Social System Design POCs

Three live POCs in one Spring Boot 3.4 / Java 21 app. Each isolates one classic social-scale
wall, lets you **watch the naive version fail**, then flip a strategy and watch the metrics
recover.

| POC | Pattern | URL |
|---|---|---|
| **#23 News Feed Fan-Out** | push / pull / hybrid | `/feed.html` |
| **#24 Notification Engine** | dedup key + time-window batching + quiet hours + multi-channel | `/notification.html` |
| **#25 Sharded Counter** | naive row-lock vs N-shard vs Redis-INCR + async flush | `/counter.html` |

All three are the same underlying move: **decouple the hot path from the expensive path.**

## Documentation

| Doc | What's in it |
|---|---|
| [ISSUE.md](ISSUE.md) | The problem each POC solves, what we're protecting, and why they live in one repo |
| [TECHNICAL.md](TECHNICAL.md) | Hard problem → solution shape → key tech by responsibility → tech debt, per POC |
| [CONSISTENCY.md](CONSISTENCY.md) | What breaks when you scale to multiple k8s pods / VMs, and the real fix |
| [`/architecture.html`](src/main/resources/static/architecture.html) | Interactive visual explainer of all three flows + scaling |

## Run

```bash
cd social-pocs
mvn spring-boot:run
```

Open <http://localhost:8080/> — the landing page links to each POC and to the
**Architecture** explainer.

## Key APIs

### Feed (`/api/feed`)
- `GET /users` — list seeded users (incl. 2 celebs with 50M+ followers).
- `POST /post?strategy={PUSH|PULL|HYBRID}` body `{authorId, text}`
- `GET /timeline/{userId}?strategy=...`
- `POST /simulate?posts=20` — bulk burst.
- `GET /metrics`

### Notifications (`/api/notify`)
- `POST /event` body `{userId, category, actorId, actorName, targetId, body}`
- `POST /burst?userId=alice&count=50&category=LIKE` — 50 likes → 1 batched delivery.
- `GET /buffer` — what's pending pre-flush.
- `GET /delivered?limit=20`
- `GET /metrics`
- `PUT /prefs` body `UserPrefs` (channels + quiet hours + batch window)

### Counter (`/api/counter`)
- `POST /incr?strategy=...` body `{key, delta}`
- `GET /value?key=...&strategy=...`
- `POST /loadtest` body `{key, threads, incrementsPerThread, strategy}` — virtual-thread storm.
- `GET /metrics`

## Notes

- All state is in-memory; restart wipes it.
- Spring `virtual.threads.enabled=true` — load tests use `Executors.newVirtualThreadPerTaskExecutor()`.
- Notification batch flusher runs every 1s (`@Scheduled`); Redis-style counter flushes every 1s too.
- The celebrity threshold is `10_000` followers (see `FeedService.CELEBRITY_THRESHOLD`).
- Sharded counter uses **32 shards** per key.
