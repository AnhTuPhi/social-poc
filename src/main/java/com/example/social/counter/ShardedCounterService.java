package com.example.social.counter;

import com.example.social.counter.CounterModels.*;
import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ShardedCounterService {

    private static final int SHARD_COUNT = 32;
    private static final long REDIS_FLUSH_MS = 1_000;

    // Naive: single lock per key (simulates DB row lock)
    private final Map<String, ReentrantLock> naiveLocks = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> naiveValues = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> naiveContentions = new ConcurrentHashMap<>();

    // Sharded: many rows summed on read
    private final Map<String, AtomicLong[]> shards = new ConcurrentHashMap<>();

    // Redis-style: in-memory hot counter, periodically flushed to DB
    private final Map<String, AtomicLong> hotCounters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> persistedValues = new ConcurrentHashMap<>();
    private final AtomicLong lastFlushTs = new AtomicLong(System.currentTimeMillis());

    // Metrics per strategy
    private final Map<Strategy, AtomicLong> totalOps = new EnumMap<>(Strategy.class);
    private final Map<Strategy, AtomicLong> totalContentions = new EnumMap<>(Strategy.class);
    private final Map<Strategy, AtomicLong> latencySumMicros = new EnumMap<>(Strategy.class);

    @PostConstruct
    public void init() {
        for (Strategy s : Strategy.values()) {
            totalOps.put(s, new AtomicLong(0));
            totalContentions.put(s, new AtomicLong(0));
            latencySumMicros.put(s, new AtomicLong(0));
        }
    }

    public IncrementResponse increment(String key, int delta, Strategy strategy) {
        long start = System.nanoTime();
        long newValue;
        boolean contended = false;

        switch (strategy) {
            case NAIVE_ROW_LOCK -> {
                ReentrantLock lock = naiveLocks.computeIfAbsent(key, k -> new ReentrantLock());
                AtomicLong val = naiveValues.computeIfAbsent(key, k -> new AtomicLong(0));
                if (lock.isLocked()) {
                    contended = true;
                    naiveContentions.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
                    totalContentions.get(strategy).incrementAndGet();
                }
                lock.lock();
                try {
                    // simulate IO of a DB write — a few microseconds
                    busyMicros(5);
                    newValue = val.addAndGet(delta);
                } finally {
                    lock.unlock();
                }
            }
            case SHARDED -> {
                AtomicLong[] arr = shards.computeIfAbsent(key, k -> {
                    AtomicLong[] a = new AtomicLong[SHARD_COUNT];
                    for (int i = 0; i < SHARD_COUNT; i++) a[i] = new AtomicLong(0);
                    return a;
                });
                int idx = ThreadLocalRandom.current().nextInt(SHARD_COUNT);
                arr[idx].addAndGet(delta);
                newValue = -1; // shard-local; clients read via GET /value to sum
            }
            case REDIS_FLUSH -> {
                AtomicLong hot = hotCounters.computeIfAbsent(key, k -> new AtomicLong(0));
                newValue = hot.addAndGet(delta);
                persistedValues.computeIfAbsent(key, k -> new AtomicLong(0));
            }
        }

        long latency = (System.nanoTime() - start) / 1_000;
        totalOps.get(strategy).incrementAndGet();
        latencySumMicros.get(strategy).addAndGet(latency);

        return new IncrementResponse(key, strategy, newValue, latency, contended);
    }

    public CounterValue read(String key, Strategy strategy) {
        long v = switch (strategy) {
            case NAIVE_ROW_LOCK -> naiveValues.getOrDefault(key, new AtomicLong(0)).get();
            case SHARDED -> {
                AtomicLong[] arr = shards.get(key);
                yield arr == null ? 0 : sumShards(arr);
            }
            case REDIS_FLUSH -> {
                long hot = hotCounters.getOrDefault(key, new AtomicLong(0)).get();
                long persisted = persistedValues.getOrDefault(key, new AtomicLong(0)).get();
                yield Math.max(hot, persisted); // hot is always >= persisted
            }
        };
        return new CounterValue(key, v, strategy);
    }

    private long sumShards(AtomicLong[] arr) {
        long sum = 0;
        for (AtomicLong a : arr) sum += a.get();
        return sum;
    }

    @Scheduled(fixedDelay = REDIS_FLUSH_MS)
    public void flushHotToPersistent() {
        for (var e : hotCounters.entrySet()) {
            String key = e.getKey();
            long v = e.getValue().get();
            persistedValues.computeIfAbsent(key, k -> new AtomicLong(0)).set(v);
        }
        lastFlushTs.set(System.currentTimeMillis());
    }

    public LoadTestResult loadTest(LoadTestRequest req) throws InterruptedException {
        AtomicLong contention = new AtomicLong(0);
        AtomicLong latencySum = new AtomicLong(0);
        AtomicLong maxLatency = new AtomicLong(0);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(req.threads());

            long globalStart = System.nanoTime();

            for (int t = 0; t < req.threads(); t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < req.incrementsPerThread(); i++) {
                            IncrementResponse r = increment(req.key(), 1, req.strategy());
                            if (r.contended()) contention.incrementAndGet();
                            latencySum.addAndGet(r.latencyMicros());
                            maxLatency.accumulateAndGet(r.latencyMicros(), Math::max);
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            done.await();

            long elapsed = (System.nanoTime() - globalStart) / 1_000_000;
            long expected = (long) req.threads() * req.incrementsPerThread();
            long actual = read(req.key(), req.strategy()).value();
            long totalOpsHere = expected;
            double ops = elapsed == 0 ? 0 : (totalOpsHere * 1000.0 / elapsed);
            double avgLat = totalOpsHere == 0 ? 0 : (double) latencySum.get() / totalOpsHere;
            boolean correct = actual >= expected;  // sharded may show slightly delayed but sum is right

            return new LoadTestResult(req.key(), req.strategy(), expected, actual, totalOpsHere,
                elapsed, ops, avgLat, maxLatency.get(), contention.get(), correct);
        }
    }

    public List<StrategyMetrics> metricsAll() {
        List<StrategyMetrics> out = new ArrayList<>();
        for (Strategy s : Strategy.values()) {
            long ops = totalOps.get(s).get();
            long cont = totalContentions.get(s).get();
            double avg = ops == 0 ? 0 : (double) latencySumMicros.get(s).get() / ops;
            long persisted = switch (s) {
                case REDIS_FLUSH -> persistedValues.values().stream().mapToLong(AtomicLong::get).sum();
                case NAIVE_ROW_LOCK -> naiveValues.values().stream().mapToLong(AtomicLong::get).sum();
                case SHARDED -> shards.values().stream().mapToLong(this::sumShards).sum();
            };
            out.add(new StrategyMetrics(s, ops, cont, avg, persisted));
        }
        return out;
    }

    public long lastFlushAgoMs() {
        return System.currentTimeMillis() - lastFlushTs.get();
    }

    public void reset() {
        naiveLocks.clear();
        naiveValues.clear();
        naiveContentions.clear();
        shards.clear();
        hotCounters.clear();
        persistedValues.clear();
        for (Strategy s : Strategy.values()) {
            totalOps.get(s).set(0);
            totalContentions.get(s).set(0);
            latencySumMicros.get(s).set(0);
        }
    }

    private static void busyMicros(long micros) {
        long end = System.nanoTime() + micros * 1_000;
        while (System.nanoTime() < end) {
            Thread.onSpinWait();
        }
    }
}
