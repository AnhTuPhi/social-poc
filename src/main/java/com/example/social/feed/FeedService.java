package com.example.social.feed;

import com.example.social.feed.FeedModels.*;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class FeedService {

    public static final int CELEBRITY_THRESHOLD = 10_000;
    private static final int MAX_FEED_ITEMS = 50;

    private final Map<Long, User> users = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> followers = new ConcurrentHashMap<>();
    private final Map<Long, Set<Long>> following = new ConcurrentHashMap<>();
    private final Map<Long, List<Post>> authorPosts = new ConcurrentHashMap<>();
    private final Map<Long, Deque<FeedItem>> userFeeds = new ConcurrentHashMap<>();

    private final AtomicLong userIdSeq = new AtomicLong(0);
    private final AtomicLong postIdSeq = new AtomicLong(0);

    private final AtomicLong pushWrites = new AtomicLong(0);
    private final AtomicLong pullMerges = new AtomicLong(0);
    private final AtomicLong celebrityPosts = new AtomicLong(0);
    private final AtomicLong normalPosts = new AtomicLong(0);
    private final AtomicLong totalReads = new AtomicLong(0);

    private final Map<Strategy, AtomicLong> latencySum = new EnumMap<>(Strategy.class);
    private final Map<Strategy, AtomicLong> latencyCount = new EnumMap<>(Strategy.class);

    public FeedService() {
        for (Strategy s : Strategy.values()) {
            latencySum.put(s, new AtomicLong(0));
            latencyCount.put(s, new AtomicLong(0));
        }
        seed();
    }

    private void seed() {
        User alice = createUser("Alice (normal)", 50);
        User bob = createUser("Bob (normal)", 120);
        User carol = createUser("Carol (normal)", 8_000);
        User taylor = createUser("Taylor (celebrity)", 50_000_000);
        User elon = createUser("Elon (celebrity)", 180_000_000);

        for (long uid = 1; uid <= 100; uid++) {
            createUser("Fan #" + uid, 5);
        }

        // everyone follows the celebrities
        users.keySet().forEach(uid -> {
            if (uid != taylor.id()) follow(uid, taylor.id());
            if (uid != elon.id()) follow(uid, elon.id());
        });
        // mutual follow among normal users
        follow(alice.id(), bob.id());
        follow(bob.id(), alice.id());
        follow(alice.id(), carol.id());
        follow(carol.id(), alice.id());
    }

    public synchronized User createUser(String name, int followerCount) {
        long id = userIdSeq.incrementAndGet();
        boolean celeb = followerCount >= CELEBRITY_THRESHOLD;
        User u = new User(id, name, followerCount, celeb);
        users.put(id, u);
        followers.put(id, ConcurrentHashMap.newKeySet());
        following.put(id, ConcurrentHashMap.newKeySet());
        authorPosts.put(id, Collections.synchronizedList(new ArrayList<>()));
        userFeeds.put(id, new ArrayDeque<>());
        return u;
    }

    public void follow(long followerId, long authorId) {
        followers.computeIfAbsent(authorId, k -> ConcurrentHashMap.newKeySet()).add(followerId);
        following.computeIfAbsent(followerId, k -> ConcurrentHashMap.newKeySet()).add(authorId);
    }

    public List<User> listUsers() {
        return users.values().stream()
            .sorted(Comparator.comparingInt(User::followerCount).reversed())
            .toList();
    }

    public PostResponse post(PostRequest req, Strategy strategy) {
        long start = System.nanoTime();
        User author = users.get(req.authorId());
        if (author == null) throw new IllegalArgumentException("Unknown author " + req.authorId());

        long postId = postIdSeq.incrementAndGet();
        Post post = new Post(postId, author.id(), author.name(), req.text(), Instant.now());
        authorPosts.get(author.id()).add(post);

        int writes = 0;
        Strategy used = strategy;

        if (strategy == Strategy.PUSH) {
            writes = fanOutPush(post, author);
        } else if (strategy == Strategy.PULL) {
            writes = 0; // no fan-out, readers pull
        } else { // HYBRID
            if (author.celebrity()) {
                writes = 0; // celebs use pull
                used = Strategy.HYBRID;
            } else {
                writes = fanOutPush(post, author);
                used = Strategy.HYBRID;
            }
        }

        if (author.celebrity()) celebrityPosts.incrementAndGet();
        else normalPosts.incrementAndGet();

        long latency = System.nanoTime() - start;
        latencySum.get(strategy).addAndGet(latency / 1_000);
        latencyCount.get(strategy).incrementAndGet();

        return new PostResponse(postId, used, writes, latency / 1_000);
    }

    private int fanOutPush(Post post, User author) {
        Set<Long> fans = followers.getOrDefault(author.id(), Set.of());
        int writes = 0;
        for (Long fanId : fans) {
            Deque<FeedItem> feed = userFeeds.get(fanId);
            if (feed == null) continue;
            synchronized (feed) {
                feed.addFirst(new FeedItem(post.id(), author.id(), author.name(), post.text(), post.createdAt(), "PUSH"));
                while (feed.size() > MAX_FEED_ITEMS) feed.removeLast();
            }
            writes++;
        }
        pushWrites.addAndGet(writes);
        return writes;
    }

    public FeedResponse readFeed(long userId, Strategy strategy) {
        long start = System.nanoTime();
        totalReads.incrementAndGet();

        List<FeedItem> result;
        int merges = 0;

        if (strategy == Strategy.PUSH) {
            result = new ArrayList<>(userFeeds.getOrDefault(userId, new ArrayDeque<>()));
        } else if (strategy == Strategy.PULL) {
            result = pullFromAllAuthors(userId);
            merges = following.getOrDefault(userId, Set.of()).size();
            pullMerges.addAndGet(merges);
        } else { // HYBRID
            // Pre-pushed items from normal authors are already in userFeeds.
            // Pull recent posts from celebrity authors and merge.
            Deque<FeedItem> precomputed = userFeeds.getOrDefault(userId, new ArrayDeque<>());
            List<FeedItem> mergeList = new ArrayList<>(precomputed);
            Set<Long> follows = following.getOrDefault(userId, Set.of());
            for (Long authorId : follows) {
                User a = users.get(authorId);
                if (a != null && a.celebrity()) {
                    List<Post> recent = recentPostsFor(authorId, 10);
                    for (Post p : recent) {
                        mergeList.add(new FeedItem(p.id(), p.authorId(), p.authorName(), p.text(), p.createdAt(), "PULL-celeb"));
                    }
                    merges++;
                }
            }
            mergeList.sort(Comparator.comparing(FeedItem::createdAt).reversed());
            result = mergeList.stream().limit(MAX_FEED_ITEMS).toList();
            pullMerges.addAndGet(merges);
        }

        long latency = System.nanoTime() - start;
        latencySum.get(strategy).addAndGet(latency / 1_000);
        latencyCount.get(strategy).incrementAndGet();

        return new FeedResponse(userId, strategy, result, merges, latency / 1_000);
    }

    private List<FeedItem> pullFromAllAuthors(long userId) {
        Set<Long> follows = following.getOrDefault(userId, Set.of());
        List<FeedItem> merged = new ArrayList<>();
        for (Long authorId : follows) {
            List<Post> recent = recentPostsFor(authorId, 10);
            User a = users.get(authorId);
            String name = a != null ? a.name() : "unknown";
            for (Post p : recent) {
                merged.add(new FeedItem(p.id(), p.authorId(), name, p.text(), p.createdAt(), "PULL"));
            }
        }
        merged.sort(Comparator.comparing(FeedItem::createdAt).reversed());
        return merged.stream().limit(MAX_FEED_ITEMS).toList();
    }

    private List<Post> recentPostsFor(long authorId, int limit) {
        List<Post> all = authorPosts.getOrDefault(authorId, List.of());
        synchronized (all) {
            int from = Math.max(0, all.size() - limit);
            return new ArrayList<>(all.subList(from, all.size()));
        }
    }

    public Metrics metrics() {
        return new Metrics(
            postIdSeq.get(),
            totalReads.get(),
            pushWrites.get(),
            pullMerges.get(),
            celebrityPosts.get(),
            normalPosts.get(),
            avgMs(Strategy.PUSH),
            avgMs(Strategy.PULL),
            avgMs(Strategy.HYBRID),
            CELEBRITY_THRESHOLD
        );
    }

    private double avgMs(Strategy s) {
        long cnt = latencyCount.get(s).get();
        if (cnt == 0) return 0.0;
        return latencySum.get(s).get() / 1000.0 / cnt;
    }

    public void reset() {
        users.clear();
        followers.clear();
        following.clear();
        authorPosts.clear();
        userFeeds.clear();
        userIdSeq.set(0);
        postIdSeq.set(0);
        pushWrites.set(0);
        pullMerges.set(0);
        celebrityPosts.set(0);
        normalPosts.set(0);
        totalReads.set(0);
        latencySum.values().forEach(a -> a.set(0));
        latencyCount.values().forEach(a -> a.set(0));
        seed();
    }
}
