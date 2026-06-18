package com.example.social.feed;

import com.example.social.feed.FeedModels.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/feed")
@CrossOrigin
public class FeedController {

    private final FeedService service;

    public FeedController(FeedService service) {
        this.service = service;
    }

    @GetMapping("/users")
    public List<User> users() {
        return service.listUsers();
    }

    @PostMapping("/post")
    public PostResponse post(@RequestParam("strategy") Strategy strategy, @RequestBody PostRequest req) {
        return service.post(req, strategy);
    }

    @GetMapping("/timeline/{userId}")
    public FeedResponse timeline(@PathVariable long userId, @RequestParam("strategy") Strategy strategy) {
        return service.readFeed(userId, strategy);
    }

    @GetMapping("/metrics")
    public Metrics metrics() {
        return service.metrics();
    }

    @PostMapping("/reset")
    public Map<String, String> reset() {
        service.reset();
        return Map.of("status", "ok");
    }

    @PostMapping("/simulate")
    public Map<String, Object> simulate(@RequestParam(defaultValue = "20") int posts) {
        var users = service.listUsers();
        int normalPosted = 0, celebPosted = 0;
        long pushFanout = 0, hybridFanout = 0;

        for (int i = 0; i < posts; i++) {
            var u = users.get(i % users.size());
            var resp = service.post(new PostRequest(u.id(), "Simulated post #" + i), Strategy.HYBRID);
            hybridFanout += resp.fanoutWrites();
            if (u.celebrity()) celebPosted++; else normalPosted++;
        }

        return Map.of(
            "postsCreated", posts,
            "normalPosts", normalPosted,
            "celebrityPosts", celebPosted,
            "totalHybridFanoutWrites", hybridFanout,
            "note", "Hybrid skipped fan-out for celebrities; pure PUSH would have done ~" +
                (long) ((celebPosted * 100_000_000L) + (normalPosted * 100L)) + " writes"
        );
    }
}
