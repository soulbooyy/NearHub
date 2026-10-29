package com.hmdp.cache;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.config.CacheRebuildConfig;
import com.hmdp.config.CaffeineConfig;
import com.hmdp.config.RedissonConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.event.ShopCacheInvalidationListener;
import com.hmdp.event.ShopUpdatedEvent;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MultiLevelCacheService;
import com.hmdp.utils.RedisData;
import org.junit.jupiter.api.*;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual Spring transaction proxy + MySQL commit/rollback + Redis. No MQ or scheduler. */
@ActiveProfiles("shop-cache-test")
@SpringBootTest(classes = ShopCacheInvalidationIntegrationTest.TestApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://${MYSQL_HOST:127.0.0.1}:${MYSQL_PORT:13306}/hmdp_cache_test?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8",
        "spring.redis.database=14",
        "cache.shop.local-ttl-seconds=60",
        "cache.shop.redis-logical-ttl-seconds=300"
})
class ShopCacheInvalidationIntegrationTest {
    @Configuration
    @Profile("shop-cache-test")
    @EnableAutoConfiguration
    @MapperScan("com.hmdp.mapper")
    @Import({ShopServiceImpl.class, ShopCacheInvalidationListener.class, MultiLevelCacheService.class,
            CacheClient.class, CacheRebuildConfig.class, CaffeineConfig.class, RedissonConfig.class})
    static class TestApplication { }

    @Autowired IShopService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource datasource;
    @Autowired PlatformTransactionManager transactions;
    @Autowired ApplicationEventPublisher events;
    @Autowired Cache<String, Object> shopLocalCache;
    @SpyBean StringRedisTemplate redis;
    static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis() * 1000);
    long id;
    String key;

    @BeforeEach void setup() {
        assertEquals("hmdp_cache_test", jdbc.queryForObject("SELECT DATABASE()", String.class));
        id = IDS.addAndGet(2);
        key = CACHE_SHOP_KEY + id;
        jdbc.update("INSERT INTO tb_shop(id,name,type_id,images,address,x,y,sold,comments,score) "
                + "VALUES (?,'old',1,'test','test',1,1,0,0,50)", id);
        RedisData cached = new RedisData();
        cached.setData(new Shop().setId(id).setName("old"));
        cached.setExpireTime(LocalDateTime.now().plusMinutes(5));
        redis.opsForValue().set(key, JSONUtil.toJsonStr(cached));
        shopLocalCache.put(key, new Shop().setId(id).setName("old"));
    }

    @AfterEach void cleanup() {
        reset(redis);
        redis.delete(Arrays.asList(key, CACHE_SHOP_KEY + (id + 1)));
        shopLocalCache.invalidate(key);
        jdbc.update("DELETE FROM tb_shop WHERE id=?", id);
    }

    String committedName() throws Exception {
        // Obtain a separate connection, not the transaction-bound JdbcTemplate connection.
        try (Connection connection = datasource.getConnection();
             PreparedStatement query = connection.prepareStatement("SELECT name FROM tb_shop WHERE id=?")) {
            query.setLong(1, id);
            try (ResultSet row = query.executeQuery()) { assertTrue(row.next()); return row.getString(1); }
        }
    }

    @Test void evictionRunsAfterOuterCommitAndRedisBeforeLocal() throws Exception {
        AtomicBoolean deletion = new AtomicBoolean();
        doAnswer(invocation -> {
            assertEquals("new", committedName());
            assertNotNull(shopLocalCache.getIfPresent(key)); // Redis deletion occurs first
            deletion.set(true);
            return invocation.callRealMethod();
        }).when(redis).delete(key);
        new TransactionTemplate(transactions).execute(status -> {
            assertTrue(service.update(new Shop().setId(id).setName("new")).getSuccess());
            assertFalse(deletion.get());
            assertNotNull(redis.opsForValue().get(key));
            assertNotNull(shopLocalCache.getIfPresent(key));
            try { assertEquals("old", committedName()); }
            catch (Exception e) { throw new RuntimeException(e); }
            return null;
        });
        assertTrue(deletion.get());
        assertNull(redis.opsForValue().get(key));
        assertNull(shopLocalCache.getIfPresent(key));
    }

    @Test void rollbackKeepsBothCachesAndOldDatabaseValue() throws Exception {
        new TransactionTemplate(transactions).execute(status -> {
            assertTrue(service.update(new Shop().setId(id).setName("rolled back")).getSuccess());
            status.setRollbackOnly();
            return null;
        });
        assertEquals("old", committedName());
        verify(redis, never()).delete(key);
        assertNotNull(redis.opsForValue().get(key));
        assertNotNull(shopLocalCache.getIfPresent(key));
    }

    @Test void redisDeleteFailureDoesNotUndoCommitAndStillClearsLocal() throws Exception {
        doThrow(new RedisConnectionFailureException("injected deletion failure")).when(redis).delete(key);
        Result result = service.update(new Shop().setId(id).setName("committed"));
        assertTrue(result.getSuccess());
        assertEquals("committed", committedName());
        assertNotNull(redis.opsForValue().get(key)); // failure was real to the caller, no hidden retry
        assertNull(shopLocalCache.getIfPresent(key));
    }

    @Test void missingShopAndMissingIdDoNotPublishInvalidations() {
        assertFalse(service.update(new Shop().setId(id + 1).setName("missing")).getSuccess());
        assertFalse(service.update(new Shop().setName("missing id")).getSuccess());
        verify(redis, never()).delete(CACHE_SHOP_KEY + (id + 1));
        verify(redis, never()).delete(key);
    }

    @Test void eventWithoutTransactionDoesNotEvict() {
        events.publishEvent(new ShopUpdatedEvent(id));
        verify(redis, never()).delete(key);
        assertNotNull(shopLocalCache.getIfPresent(key));
    }

    @Test void anotherJvmLocalCacheReliesOnWriteTtlRatherThanNotification() throws Exception {
        assertEquals(60, shopLocalCache.policy().expireAfterWrite().get().getExpiresAfter(TimeUnit.SECONDS));
        Cache<String, Object> otherJvm = new CaffeineConfig().shopLocalCache(1);
        otherJvm.put(key, new Shop().setId(id).setName("old"));
        assertTrue(service.update(new Shop().setId(id).setName("new")).getSuccess());
        assertNull(shopLocalCache.getIfPresent(key));
        assertNotNull(otherJvm.getIfPresent(key));
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        // Repeated reads do not extend expireAfterWrite.
        while (otherJvm.getIfPresent(key) != null && System.nanoTime() < end) Thread.sleep(20);
        assertNull(otherJvm.getIfPresent(key));
    }

    @Test void mainReadUsesConfiguredShortLogicalTtlWithoutPhysicalExpiry() {
        redis.delete(key);
        shopLocalCache.invalidate(key);
        assertTrue(service.queryById(id).getSuccess());
        RedisData cached = JSONUtil.toBean(redis.opsForValue().get(key), RedisData.class);
        long remaining = Duration.between(LocalDateTime.now(), cached.getExpireTime()).getSeconds();
        assertTrue(remaining >= 299 && remaining <= 360, "logical TTL seconds=" + remaining);
        assertEquals(Long.valueOf(-1), redis.getExpire(key)); // logical expiry still permits stale reads
    }
}
