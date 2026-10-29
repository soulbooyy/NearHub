package com.hmdp.cache;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.config.CacheRebuildConfig;
import com.hmdp.entity.Shop;
import com.hmdp.service.CacheRebuildBusyException;
import com.hmdp.utils.MultiLevelCacheService;
import com.hmdp.utils.RedisData;
import org.junit.jupiter.api.*;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import static com.hmdp.utils.RedisConstants.CACHE_REBUILD_LOCK_KEY;
import static org.junit.jupiter.api.Assertions.*;

/** Real Redis DB 14, two independent Redisson clients and JVM-local caches; no MQ or DB mutations.
 * DB fallback is a controlled function so query timing/failure can be reproduced with latches. */
class MultiLevelCacheIntegrationTest {
    static RedissonClient first, second;
    static LettuceConnectionFactory connections;
    static StringRedisTemplate redis;
    final String prefix = "test:cache-rebuild:" + UUID.randomUUID() + ":";
    final List<String> keys = new ArrayList<>();
    ThreadPoolTaskExecutor worker;
    ExecutorService callers;
    Cache<String, Object> localA, localB;
    MultiLevelCacheService a, b;

    @BeforeAll static void connect() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "16379"));
        String password = System.getenv("REDIS_PASSWORD");
        RedisStandaloneConfiguration standalone = new RedisStandaloneConfiguration(host, port);
        standalone.setDatabase(14);
        if (password != null && !password.isEmpty()) standalone.setPassword(password);
        connections = new LettuceConnectionFactory(standalone);
        connections.afterPropertiesSet();
        redis = new StringRedisTemplate(connections);
        redis.afterPropertiesSet();
        Config config = new Config();
        config.setThreads(2).setNettyThreads(2).setLockWatchdogTimeout(3000);
        config.useSingleServer().setAddress("redis://" + host + ":" + port).setDatabase(14)
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(4)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(2);
        if (password != null && !password.isEmpty()) config.useSingleServer().setPassword(password);
        first = Redisson.create(config);
        second = Redisson.create(config);
    }

    @AfterAll static void disconnect() {
        first.shutdown();
        second.shutdown();
        connections.destroy();
    }

    @BeforeEach void setup() {
        worker = new CacheRebuildConfig().cacheRebuildExecutor(1, 1, 3);
        worker.initialize();
        callers = Executors.newFixedThreadPool(4);
        localA = Caffeine.newBuilder().maximumSize(100).build();
        localB = Caffeine.newBuilder().maximumSize(100).build();
        a = service(first, localA);
        b = service(second, localB);
    }

    MultiLevelCacheService service(RedissonClient client, Cache<String, Object> local) {
        MultiLevelCacheService service = new MultiLevelCacheService();
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "shopLocalCache", local);
        ReflectionTestUtils.setField(service, "redissonClient", client);
        ReflectionTestUtils.setField(service, "cacheRebuildExecutor", worker);
        ReflectionTestUtils.setField(service, "lockWaitMs", 200L);
        service.validateConfiguration();
        return service;
    }

    @AfterEach void cleanup() throws Exception {
        worker.shutdown();
        callers.shutdownNow();
        assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS));
        redis.delete(keys);
    }

    String key(long id) {
        String key = prefix + id;
        if (!keys.contains(key)) {
            keys.add(key);
            keys.add(CACHE_REBUILD_LOCK_KEY + key);
        }
        return key;
    }
    RLock lock(RedissonClient client, long id) { return client.getLock(CACHE_REBUILD_LOCK_KEY + key(id)); }
    Shop shop(long id, String name) { return new Shop().setId(id).setName(name); }
    Shop query(MultiLevelCacheService service, long id, Function<Long, Shop> loader) {
        key(id);
        return service.queryWithMultiLevel(prefix, id, Shop.class, loader, 30L, TimeUnit.MINUTES);
    }
    void cache(long id, String name, boolean stale) {
        RedisData value = new RedisData();
        value.setData(shop(id, name));
        value.setExpireTime(LocalDateTime.now().plusMinutes(stale ? -1 : 30));
        redis.opsForValue().set(key(id), JSONUtil.toJsonStr(value));
    }
    void until(BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }
    void gate(CountDownLatch gate) {
        try {
            if (!gate.await(20, TimeUnit.SECONDS)) throw new AssertionError("gate timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
    void idle() throws Exception {
        // A just-created worker may not yet be counted as active. Await task cleanup too.
        until(() -> ((Set<?>) ReflectionTestUtils.getField(a, "rebuilding")).isEmpty()
                && ((Set<?>) ReflectionTestUtils.getField(b, "rebuilding")).isEmpty()
                && worker.getActiveCount() == 0 && worker.getThreadPoolExecutor().getQueue().isEmpty());
    }

    @Test void failedAcquisitionDoesNotDeleteOtherOwnersLock() {
        RLock held = lock(second, 1);
        held.lock();
        try {
            AtomicInteger db = new AtomicInteger();
            long start = System.nanoTime();
            assertThrows(CacheRebuildBusyException.class, () -> query(a, 1, id -> {
                db.incrementAndGet(); return shop(id, "unexpected");
            }));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsed >= 150 && elapsed < 3000, "contention wait ms=" + elapsed);
            assertTrue(held.isHeldByCurrentThread());
            assertEquals(0, db.get());
        } finally { held.unlock(); }
    }

    @Test void timeoutRecheckAcceptsValueOrNegativeCache() throws Exception {
        for (int state = 0; state < 3; state++) {
            boolean negative = state == 1;
            boolean stale = state == 2;
            long id = state + 1;
            RLock held = lock(second, id);
            held.lock();
            try {
                CountDownLatch started = new CountDownLatch(1);
                Future<Shop> result = callers.submit(() -> {
                    started.countDown();
                    return query(a, id, ignored -> { throw new AssertionError("unexpected DB fallback"); });
                });
                assertTrue(started.await(2, TimeUnit.SECONDS));
                Thread.sleep(70);
                if (negative) redis.opsForValue().set(key(id), "", 1, TimeUnit.MINUTES);
                else cache(id, "published", stale);
                if (negative) assertNull(result.get(3, TimeUnit.SECONDS));
                else assertEquals("published", result.get(3, TimeUnit.SECONDS).getName());
                if (stale) {
                    idle(); // worker sees the other owner; old data still returns without 503
                    assertNull(localA.getIfPresent(key(id)));
                }
                assertTrue(held.isHeldByCurrentThread());
            } finally { held.unlock(); }
        }
    }

    @Test void independentClientsReuseSingleColdDatabaseResult() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger db = new AtomicInteger();
        Function<Long, Shop> loader = id -> {
            db.incrementAndGet(); entered.countDown(); gate(release); return shop(id, "new");
        };
        Future<Shop> owner = callers.submit(() -> query(a, 1, loader));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        Future<Shop> follower = callers.submit(() -> query(b, 1, loader));
        Thread.sleep(50);
        release.countDown();
        assertEquals("new", owner.get(3, TimeUnit.SECONDS).getName());
        assertEquals("new", follower.get(3, TimeUnit.SECONDS).getName());
        assertEquals(1, db.get());
    }

    @Test void lockDoubleCheckRecognizesNegativeCache() throws Exception {
        RLock held = lock(second, 1);
        held.lock();
        Future<Shop> result;
        try {
            result = callers.submit(() -> query(a, 1, id -> { throw new AssertionError("negative cache ignored"); }));
            Thread.sleep(70);
            redis.opsForValue().set(key(1), "", 1, TimeUnit.MINUTES);
        } finally { held.unlock(); }
        assertNull(result.get(3, TimeUnit.SECONDS));
        assertFalse(lock(first, 1).isLocked());
    }

    @Test void interruptedWaitRestoresInterruptAndDoesNotFallBack() throws Exception {
        RLock held = lock(second, 1);
        held.lock();
        ReflectionTestUtils.setField(a, "lockWaitMs", 5000L);
        AtomicBoolean restored = new AtomicBoolean();
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1), done = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            started.countDown();
            try { query(a, 1, id -> { throw new AssertionError("unlocked DB fallback"); }); }
            catch (Throwable e) { caught.set(e); restored.set(Thread.currentThread().isInterrupted()); }
            finally { done.countDown(); }
        });
        try {
            thread.start();
            assertTrue(started.await(2, TimeUnit.SECONDS));
            Thread.sleep(150);
            thread.interrupt();
            assertTrue(done.await(3, TimeUnit.SECONDS));
            assertTrue(caught.get() instanceof CacheRebuildBusyException, String.valueOf(caught.get()));
            assertTrue(restored.get());
            assertTrue(held.isHeldByCurrentThread());
        } finally { thread.interrupt(); thread.join(3000); held.unlock(); }
    }

    @Test void oldOwnerCannotUnlockSuccessorAfterLockLoss() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<Shop> old = callers.submit(() -> query(a, 1, id -> {
            entered.countDown(); gate(release); return shop(id, "old query");
        }));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        // Fault injection on this test's unique lock only; production never DELs locks.
        redis.delete(CACHE_REBUILD_LOCK_KEY + key(1));
        RLock successor = lock(second, 1);
        assertTrue(successor.tryLock(1, TimeUnit.SECONDS));
        try {
            release.countDown();
            old.get(3, TimeUnit.SECONDS);
            assertTrue(successor.isHeldByCurrentThread());
            // Deliberately no assertion that the old write was fenced: that is separate work.
        } finally { successor.unlock(); }
    }

    @Test void watchdogRenewsBeyondOldTenSecondLease() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<Shop> owner = callers.submit(() -> query(a, 1, id -> {
            entered.countDown(); gate(release); return shop(id, "slow");
        }));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        try {
            Thread.sleep(11000); // test watchdog is 3s; old implementation expired after 10s
            assertTrue(lock(second, 1).remainTimeToLive() > 0);
            assertFalse(lock(second, 1).tryLock(100, TimeUnit.MILLISECONDS));
        } finally { release.countDown(); }
        assertEquals("slow", owner.get(3, TimeUnit.SECONDS).getName());
        assertFalse(lock(second, 1).isLocked());
    }

    @Test void databaseFailureReleasesLockWithoutNegativeCaching() {
        IllegalStateException failure = new IllegalStateException("DB unavailable");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> query(a, 1, id -> { throw failure; })));
        assertFalse(lock(second, 1).isLocked());
        assertNull(redis.opsForValue().get(key(1)));
        assertEquals("recovered", query(b, 1, id -> shop(id, "recovered")).getName());
    }

    @Test void interruptedOwnerCleansLockAndPreservesOriginalFailure() {
        IllegalStateException failure = new IllegalStateException("interrupted DB operation");
        try {
            assertSame(failure, assertThrows(IllegalStateException.class, () -> query(a, 1, id -> {
                Thread.currentThread().interrupt();
                throw failure;
            })));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // do not leak the test thread's interrupt flag to JUnit
        }
        assertFalse(lock(second, 1).isLocked());
    }

    @Test void staleReturnsImmediatelyAndDeduplicatesQueuedRebuild() throws Exception {
        CountDownLatch occupied = new CountDownLatch(1), release = new CountDownLatch(1);
        worker.execute(() -> { occupied.countDown(); gate(release); });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        cache(1, "stale", true);
        AtomicInteger db = new AtomicInteger();
        AtomicBoolean sameThreadOwnsLock = new AtomicBoolean();
        Function<Long, Shop> loader = id -> {
            db.incrementAndGet();
            sameThreadOwnsLock.set(lock(first, id).isHeldByCurrentThread()
                    && Thread.currentThread().getName().startsWith("cache-rebuild-"));
            return shop(id, "fresh");
        };
        for (int i = 0; i < 5; i++) assertEquals("stale", query(a, 1, loader).getName());
        assertNull(localA.getIfPresent(key(1)));
        assertEquals(1, worker.getThreadPoolExecutor().getQueue().size());
        assertFalse(lock(second, 1).isLocked()); // no request-thread lock while queued
        localA.put(key(1), shop(1, "another local old value"));
        release.countDown(); idle();
        assertEquals(1, db.get());
        assertTrue(sameThreadOwnsLock.get());
        assertNull(localA.getIfPresent(key(1)));
        assertEquals("fresh", query(a, 1, loader).getName());
        assertFalse(lock(second, 1).isLocked());
    }

    @Test void queuedTaskDoubleChecksFreshAndNegativeResults() throws Exception {
        for (boolean negative : Arrays.asList(false, true)) {
            long id = negative ? 2 : 1;
            CountDownLatch occupied = new CountDownLatch(1), release = new CountDownLatch(1);
            worker.execute(() -> { occupied.countDown(); gate(release); });
            assertTrue(occupied.await(2, TimeUnit.SECONDS));
            cache(id, "old", true);
            AtomicInteger db = new AtomicInteger();
            query(a, id, ignored -> { db.incrementAndGet(); return shop(id, "wrong"); });
            if (negative) redis.opsForValue().set(key(id), "", 1, TimeUnit.MINUTES);
            else cache(id, "other worker", false);
            release.countDown(); idle();
            assertEquals(0, db.get());
        }
    }

    @Test void fullQueueReturnsStaleAndAllowsLaterSubmission() throws Exception {
        CountDownLatch occupied = new CountDownLatch(1), release = new CountDownLatch(1);
        worker.execute(() -> { occupied.countDown(); gate(release); });
        assertTrue(occupied.await(2, TimeUnit.SECONDS));
        worker.execute(() -> {}); // fill the one queue slot
        cache(1, "stale", true);
        AtomicInteger db = new AtomicInteger();
        Function<Long, Shop> loader = id -> { db.incrementAndGet(); return shop(id, "new"); };
        assertEquals("stale", query(a, 1, loader).getName());
        assertEquals(0, db.get());
        assertFalse(lock(second, 1).isLocked());
        release.countDown(); idle();
        assertEquals("stale", query(a, 1, loader).getName());
        idle();
        assertEquals(1, db.get());
    }

    @Test void asyncLockFailureClearsDedupAndDoesNotReleaseOtherOwner() throws Exception {
        cache(1, "stale", true);
        RLock held = lock(second, 1);
        held.lock();
        AtomicInteger db = new AtomicInteger();
        Function<Long, Shop> loader = id -> { db.incrementAndGet(); return shop(id, "new"); };
        try {
            assertEquals("stale", query(a, 1, loader).getName());
            idle();
            assertEquals(0, db.get());
            assertTrue(held.isHeldByCurrentThread());
        } finally { held.unlock(); }
        query(a, 1, loader); idle();
        assertEquals(1, db.get());
    }

    @Test void asyncFailureClearsDedupForRecovery() throws Exception {
        cache(1, "old", true);
        AtomicInteger attempts = new AtomicInteger();
        Function<Long, Shop> loader = id -> {
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("DB failed");
            return shop(id, "recovered");
        };
        query(a, 1, loader); idle();
        assertFalse(lock(second, 1).isLocked());
        assertTrue(redis.opsForValue().get(key(1)).contains("old"));
        query(a, 1, loader); idle();
        assertEquals(2, attempts.get());
        assertEquals("recovered", query(a, 1, loader).getName());
    }

    @Test void missingShopRebuildWritesNegativeAndInvalidatesLocal() throws Exception {
        cache(1, "old", true);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        query(a, 1, id -> { entered.countDown(); gate(release); return null; });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        localA.put(key(1), shop(1, "old local"));
        release.countDown(); idle();
        assertEquals("", redis.opsForValue().get(key(1)));
        assertTrue(redis.getExpire(key(1)) > 0);
        assertNull(localA.getIfPresent(key(1)));
        assertNull(query(a, 1, id -> { throw new AssertionError("negative cache missed"); }));
    }

    @Test void shutdownInterruptsWorkerAndReleasesItsLock() throws Exception {
        cache(1, "old", true);
        CountDownLatch entered = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        query(a, 1, id -> {
            entered.countDown();
            try { Thread.sleep(20000); }
            catch (InterruptedException e) { interrupted.set(true); Thread.currentThread().interrupt(); throw new RuntimeException(e); }
            return shop(id, "unexpected");
        });
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        worker.shutdown();
        assertTrue(interrupted.get());
        assertFalse(lock(second, 1).isLocked());
        assertEquals("old", query(a, 1, id -> { throw new AssertionError("closed executor accepted work"); }).getName());
    }
}
