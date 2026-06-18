package com.example.social.notification;

import com.example.social.notification.NotificationModels.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/notify")
@CrossOrigin
public class NotificationController {

    private final NotificationEngine engine;

    public NotificationController(NotificationEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/event")
    public Map<String, String> ingest(@RequestBody IncomingEvent ev) {
        engine.ingest(ev);
        return Map.of("status", "buffered");
    }

    @PostMapping("/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "alice") String userId,
                                     @RequestParam(defaultValue = "50") int count,
                                     @RequestParam(defaultValue = "LIKE") Category category) {
        String target = "post-" + System.currentTimeMillis() % 1000;
        for (int i = 0; i < count; i++) {
            String actor = "user_" + (i % 8);
            engine.ingest(new IncomingEvent(userId, category, actor, "User#" + (i % 8),
                target, "did the thing on " + target));
        }
        return Map.of("ingested", count, "userId", userId, "target", target,
            "note", "Watch the buffer collapse " + count + " events into 1 batch via dedup");
    }

    @GetMapping("/buffer")
    public List<Map<String, Object>> buffer() {
        return engine.currentBuffer().stream().map(b -> {
            Map<String, Object> m = new HashMap<>();
            m.put("userId", b.first().userId());
            m.put("category", b.first().category());
            m.put("dedupKey", b.dedupKey());
            m.put("count", b.count());
            m.put("actorNames", b.actorNames());
            m.put("firstSeen", b.firstSeen());
            return m;
        }).toList();
    }

    @GetMapping("/delivered")
    public List<Notification> delivered(@RequestParam(defaultValue = "20") int limit) {
        return engine.recentDelivered(limit);
    }

    @GetMapping("/metrics")
    public EngineMetrics metrics() {
        return engine.metrics();
    }

    @GetMapping("/prefs")
    public Collection<UserPrefs> prefs() {
        return engine.allPrefs();
    }

    @PutMapping("/prefs")
    public Map<String, String> updatePrefs(@RequestBody UserPrefs p) {
        engine.updatePrefs(p);
        return Map.of("status", "updated");
    }

    @PostMapping("/reset")
    public Map<String, String> reset() {
        engine.reset();
        return Map.of("status", "ok");
    }
}
