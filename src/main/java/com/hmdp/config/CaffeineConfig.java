package com.hmdp.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Caffeine 本地缓存（读写链路 Java 侧 L1）
 */
@Configuration
public class CaffeineConfig {

    /**
     * 商铺本地缓存：最大 10000 条，默认写入后 60 秒过期，不因访问续期。
     */
    @Bean
    public Cache<String, Object> shopLocalCache(
            @Value("${cache.shop.local-ttl-seconds:60}") long ttlSeconds) {
        if (ttlSeconds <= 0) throw new IllegalArgumentException("cache.shop.local-ttl-seconds must be > 0");
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
                .recordStats()
                .build();
    }
}
