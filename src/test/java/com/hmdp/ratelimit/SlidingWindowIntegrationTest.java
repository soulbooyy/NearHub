package com.hmdp.ratelimit;

import com.hmdp.utils.SlidingWindowRateLimiter;
import org.junit.jupiter.api.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Real Redis, unique keys in DB 14; never clears a shared database. */
class SlidingWindowIntegrationTest {
    static LettuceConnectionFactory connections;
    static StringRedisTemplate redis;
    final List<String> keys = new ArrayList<>();
    SlidingWindowRateLimiter limiter;

    @BeforeAll static void connect() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1"),
                Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "16379")));
        config.setDatabase(14);
        String password = System.getenv("REDIS_PASSWORD");
        if (password != null && !password.isEmpty()) config.setPassword(password);
        connections = new LettuceConnectionFactory(config);
        connections.afterPropertiesSet();
        redis = new StringRedisTemplate(connections);
        redis.afterPropertiesSet();
    }

    @AfterAll static void disconnect() { if (connections != null) connections.destroy(); }

    @BeforeEach void setup() {
        limiter = new SlidingWindowRateLimiter();
        ReflectionTestUtils.setField(limiter, "stringRedisTemplate", redis);
    }

    @AfterEach void cleanup() { if (!keys.isEmpty()) redis.delete(keys); }

    String key() {
        String key = "test:sliding-window:" + UUID.randomUUID();
        keys.add(key);
        return key;
    }

    @Test void concurrentSubmissionsShareAnAtomicQuota() throws Exception {
        String key = key();
        int callers = 32;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch ready = new CountDownLatch(callers), start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        // Warm the connection before racing callers; all requests must finish within one window.
        redis.hasKey(key);
        try {
            for (int i = 0; i < callers; i++) {
                results.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                    return limiter.tryAcquire(key, 30000, 5);
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            long began = System.nanoTime();
            start.countDown();
            int allowed = 0;
            for (Future<Boolean> result : results) if (result.get(10, TimeUnit.SECONDS)) allowed++;
            assertTrue(System.nanoTime() - began < TimeUnit.SECONDS.toNanos(30));
            assertEquals(5, allowed);
            assertEquals(5L, redis.opsForZSet().zCard(key));
        } finally {
            start.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test void usersHaveIndependentQuotasAndRejectionDoesNotAddRecords() {
        String first = key(), second = key();
        assertTrue(limiter.tryAcquire(first, 30000, 1));
        assertFalse(limiter.tryAcquire(first, 30000, 1));
        assertTrue(limiter.tryAcquire(second, 30000, 1));
        assertEquals(1L, redis.opsForZSet().zCard(first));
        Long ttl = redis.getExpire(first, TimeUnit.MILLISECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 30000);
    }

    @Test void exactWindowBoundaryExpiresEvenWhileKeyStillExists() {
        String key = key();
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("sliding_window.lua"));
        script.setResultType(Long.class);
        // Synthetic time keeps the inclusive eviction boundary deterministic, without sleeps.
        assertEquals(1L, redis.execute(script, Collections.singletonList(key), "10000", "1000", "1", "first"));
        assertEquals(0L, redis.execute(script, Collections.singletonList(key), "10999", "1000", "1", "blocked"));
        assertEquals(1L, redis.execute(script, Collections.singletonList(key), "11000", "1000", "1", "next"));
        assertEquals(Collections.singleton("next"), redis.opsForZSet().range(key, 0, -1));
    }

    @Test void idleWindowKeyExpiresAndNextSubmissionCanProceed() throws Exception {
        String key = key();
        assertTrue(limiter.tryAcquire(key, 200, 1));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (Boolean.TRUE.equals(redis.hasKey(key)) && System.nanoTime() < deadline) Thread.sleep(20);
        assertFalse(Boolean.TRUE.equals(redis.hasKey(key)), "window key should expire without another request");
        assertTrue(limiter.tryAcquire(key, 200, 1));
    }
}
