package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.service.CacheRebuildBusyException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/** Caffeine -> Redis -> MySQL. Locks coordinate rebuilds, not shop update consistency. */
@Slf4j
@Component
public class MultiLevelCacheService {
    @Resource private StringRedisTemplate stringRedisTemplate;
    @Resource private Cache<String, Object> shopLocalCache;
    @Resource private RedissonClient redissonClient;
    @Resource private ThreadPoolTaskExecutor cacheRebuildExecutor;
    @Value("${cache.rebuild.lock-wait-ms:200}") private long lockWaitMs;

    private final Set<String> rebuilding = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void validateConfiguration() {
        if (lockWaitMs < 0) throw new IllegalArgumentException("cache.rebuild.lock-wait-ms must be >= 0");
    }

    @SuppressWarnings("unchecked")
    public <R, ID> R queryWithMultiLevel(String keyPrefix, ID id, Class<R> type,
                                        Function<ID, R> dbFallback, Long ttl, TimeUnit unit) {
        String key = keyPrefix + id;
        R local = (R) shopLocalCache.getIfPresent(key);
        if (local != null) return local;
        CacheRead<R> cached = readRedis(key, type);
        if (!cached.present) cached = loadCold(key, id, type, dbFallback, ttl, unit);
        if (cached.stale) {
            rebuildAsync(key, id, type, dbFallback, ttl, unit);
            // Do not renew Caffeine's lifetime using logically expired Redis data.
        } else if (cached.data != null) {
            shopLocalCache.put(key, cached.data);
        }
        return cached.data;
    }

    /** Evicts Redis first, then this JVM even if Redis deletion fails. */
    public void evict(String keyPrefix, Object id) {
        String key = keyPrefix + id;
        try {
            stringRedisTemplate.delete(key);
        } finally {
            shopLocalCache.invalidate(key);
        }
    }

    private <R, ID> CacheRead<R> loadCold(String key, ID id, Class<R> type,
                                         Function<ID, R> dbFallback, Long ttl, TimeUnit unit) {
        RLock lock = redissonClient.getLock(CACHE_REBUILD_LOCK_KEY + key);
        boolean acquired = false;
        try {
            // No fixed leaseTime: use Redisson watchdog renewal. This is a contention budget,
            // not an end-to-end deadline for Redis IO or the DB query.
            acquired = lock.tryLock(lockWaitMs, TimeUnit.MILLISECONDS);
            CacheRead<R> cached = readRedis(key, type);
            if (cached.present) return cached; // includes empty-string negative cache
            if (!acquired) throw new CacheRebuildBusyException("Cache rebuild busy: " + key);
            R data = dbFallback.apply(id);
            writeResult(key, data, ttl, unit);
            return new CacheRead<>(true, false, data);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheRebuildBusyException("Cache rebuild interrupted: " + key, e);
        } finally {
            if (acquired) releaseOwnedLock(lock);
        }
    }

    private <R> CacheRead<R> readRedis(String key, Class<R> type) {
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json == null) return new CacheRead<>(false, false, null);
        if (StrUtil.isBlank(json)) return new CacheRead<>(true, false, null);
        RedisData value = JSONUtil.toBean(json, RedisData.class);
        R data = JSONUtil.toBean((cn.hutool.json.JSONObject) value.getData(), type);
        return new CacheRead<>(true, !value.getExpireTime().isAfter(LocalDateTime.now()), data);
    }

    private <R, ID> void rebuildAsync(String key, ID id, Class<R> type,
                                      Function<ID, R> dbFallback, Long ttl, TimeUnit unit) {
        if (!rebuilding.add(key)) return;
        boolean submitted = false;
        try {
            cacheRebuildExecutor.execute(() -> {
                RLock lock = null;
                boolean acquired = false;
                try {
                    // The worker owns the entire lock lifecycle; the request thread never locks.
                    lock = redissonClient.getLock(CACHE_REBUILD_LOCK_KEY + key);
                    acquired = lock.tryLock(0, TimeUnit.MILLISECONDS);
                    if (!acquired) return;
                    CacheRead<R> current = readRedis(key, type);
                    if (current.present && !current.stale) return;
                    R data = dbFallback.apply(id);
                    writeResult(key, data, ttl, unit);
                    shopLocalCache.invalidate(key);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("CACHE_REBUILD_INTERRUPTED: {}", key);
                } catch (Exception e) {
                    log.error("CACHE_REBUILD_FAILED: {}", key, e);
                } finally {
                    try {
                        if (acquired) releaseOwnedLock(lock);
                    } finally {
                        rebuilding.remove(key);
                    }
                }
            });
            submitted = true;
        } catch (RejectedExecutionException e) {
            // Fail open for a request that already has stale data. Never run DB IO on this thread.
            log.warn("CACHE_REBUILD_REJECTED: {}", key);
        } finally {
            if (!submitted) rebuilding.remove(key);
        }
    }

    private void writeResult(String key, Object value, Long ttl, TimeUnit unit) {
        if (value == null) {
            stringRedisTemplate.opsForValue().set(key, "",
                    CACHE_NULL_TTL + ThreadLocalRandom.current().nextInt(3), TimeUnit.MINUTES);
            return;
        }
        RedisData cached = new RedisData();
        cached.setData(value);
        long baseSec = unit.toSeconds(ttl);
        long jitter = (long) (baseSec * 0.2 * ThreadLocalRandom.current().nextDouble());
        cached.setExpireTime(LocalDateTime.now().plusSeconds(baseSec + jitter));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(cached));
        // Watchdog is not fencing: a paused former owner can still write after losing its lock.
    }

    private void releaseOwnedLock(RLock lock) {
        // Redisson 3.13 synchronous commands fail immediately on an interrupted thread.
        // Allow owned-lock cleanup, then restore the caller's interruption status.
        boolean interrupted = Thread.interrupted();
        try {
            if (lock.isHeldByCurrentThread()) lock.unlock();
            else log.warn("CACHE_REBUILD_LOCK_LOST: {}", lock.getName());
        } catch (RuntimeException e) {
            // unlock itself also validates ownership atomically. Do not mask a DB/Redis failure
            // or delete a successor's lock if ownership changed after the check above.
            log.warn("CACHE_REBUILD_UNLOCK_FAILED: {}", lock.getName(), e);
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private static class CacheRead<R> {
        final boolean present;
        final boolean stale;
        final R data;
        CacheRead(boolean present, boolean stale, R data) {
            this.present = present;
            this.stale = stale;
            this.data = data;
        }
    }
}
