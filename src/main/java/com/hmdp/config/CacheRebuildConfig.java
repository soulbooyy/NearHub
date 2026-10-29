package com.hmdp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class CacheRebuildConfig {
    @Bean
    public ThreadPoolTaskExecutor cacheRebuildExecutor(
            @Value("${cache.rebuild.threads:10}") int threads,
            @Value("${cache.rebuild.queue-capacity:200}") int capacity,
            @Value("${cache.rebuild.shutdown-wait-seconds:5}") int shutdownSeconds) {
        if (threads < 1 || capacity < 0 || shutdownSeconds < 0) {
            throw new IllegalArgumentException("Invalid cache rebuild executor configuration");
        }
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(capacity);
        executor.setThreadNamePrefix("cache-rebuild-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.setAwaitTerminationSeconds(shutdownSeconds);
        return executor;
    }
}
