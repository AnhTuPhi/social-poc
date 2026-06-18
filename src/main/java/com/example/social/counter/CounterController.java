package com.example.social.counter;

import com.example.social.counter.CounterModels.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/counter")
@CrossOrigin
public class CounterController {

    private final ShardedCounterService service;

    public CounterController(ShardedCounterService service) {
        this.service = service;
    }

    @PostMapping("/incr")
    public IncrementResponse incr(@RequestParam Strategy strategy, @RequestBody IncrementRequest req) {
        return service.increment(req.key(), req.delta(), strategy);
    }

    @GetMapping("/value")
    public CounterValue value(@RequestParam String key, @RequestParam Strategy strategy) {
        return service.read(key, strategy);
    }

    @PostMapping("/loadtest")
    public LoadTestResult loadTest(@RequestBody LoadTestRequest req) throws InterruptedException {
        return service.loadTest(req);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        List<StrategyMetrics> all = service.metricsAll();
        return Map.of(
            "strategies", all,
            "lastFlushAgoMs", service.lastFlushAgoMs()
        );
    }

    @PostMapping("/reset")
    public Map<String, String> reset() {
        service.reset();
        return Map.of("status", "ok");
    }
}
