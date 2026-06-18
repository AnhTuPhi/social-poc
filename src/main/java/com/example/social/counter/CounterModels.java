package com.example.social.counter;

public class CounterModels {
    public enum Strategy { NAIVE_ROW_LOCK, SHARDED, REDIS_FLUSH }

    public record IncrementRequest(String key, int delta) {}

    public record IncrementResponse(String key, Strategy strategy, long newValue, long latencyMicros, boolean contended) {}

    public record CounterValue(String key, long value, Strategy strategy) {}

    public record LoadTestRequest(String key, int threads, int incrementsPerThread, Strategy strategy) {}

    public record LoadTestResult(
        String key,
        Strategy strategy,
        long expectedFinal,
        long actualFinal,
        long totalIncrements,
        long elapsedMs,
        double opsPerSec,
        double avgLatencyMicros,
        long maxLatencyMicros,
        long contentionEvents,
        boolean correct
    ) {}

    public record StrategyMetrics(
        Strategy strategy,
        long totalOps,
        long totalContentions,
        double avgLatencyMicros,
        long persistedValue
    ) {}
}
