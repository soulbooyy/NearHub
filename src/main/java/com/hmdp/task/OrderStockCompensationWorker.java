package com.hmdp.task;

import com.hmdp.entity.OrderStockCompensation;
import com.hmdp.mapper.OrderStockCompensationMapper;
import com.hmdp.service.RedisStockReturnService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.UUID;

/** Short DB leases; no DB transaction spans Redis IO. Replays use a permanent Redis marker. */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderStockCompensationWorker {
    private final OrderStockCompensationMapper tasks;
    private final RedisStockReturnService redisReturns;

    @Value("${seckill.compensation.batch-size:100}") private int batchSize;
    @Value("${seckill.compensation.max-attempts:10}") private int maxAttempts;
    @Value("${seckill.compensation.lease-seconds:60}") private int leaseSeconds;
    @Value("${seckill.compensation.retry-base-seconds:30}") private int retryBaseSeconds;
    @Value("${seckill.compensation.retry-max-seconds:600}") private int retryMaxSeconds;

    @PostConstruct
    public void validateConfiguration() {
        if (batchSize < 1 || maxAttempts < 1 || leaseSeconds < 1 || retryBaseSeconds < 1
                || retryMaxSeconds < retryBaseSeconds) {
            throw new IllegalArgumentException("Invalid stock compensation retry/lease configuration");
        }
    }

    @Scheduled(fixedDelayString = "${seckill.compensation.poll-ms:5000}")
    public void runBatch() {
        for (Long orderId : tasks.findDueIds(batchSize)) {
            try {
                process(orderId);
            } catch (Exception e) {
                // DB outage may prevent recording failure; the lease allows recovery after restart.
                log.error("STOCK_COMPENSATION_RETRY: orderId={}", orderId, e);
            }
        }
    }

    public void process(Long orderId) {
        if (tasks.exhaust(orderId, maxAttempts) == 1) {
            log.error("STOCK_COMPENSATION_MANUAL_REVIEW: retry budget exhausted, orderId={}", orderId);
            return;
        }
        String token = UUID.randomUUID().toString();
        if (tasks.claim(orderId, token, leaseSeconds, maxAttempts) != 1) {
            return;
        }
        OrderStockCompensation task = tasks.selectById(orderId);
        if (task == null || !token.equals(task.getLeaseToken())) {
            return;
        }
        try {
            Long result = redisReturns.returnStock(task);
            if (Long.valueOf(0).equals(result) || Long.valueOf(1).equals(result)) {
                tasks.complete(orderId, token);
            } else if (Long.valueOf(2).equals(result) || Long.valueOf(3).equals(result) || Long.valueOf(4).equals(result)) {
                recordFailure(task, token, "Redis state requires review, script result=" + result, true);
            } else {
                recordFailure(task, token, "Uncertain Redis result=" + result, false);
            }
        } catch (Exception e) {
            // Includes lost Redis response or lost DONE acknowledgement. Never undo Redis here.
            recordFailure(task, token, e.getClass().getSimpleName() + ": " + e.getMessage(), false);
        }
    }

    private void recordFailure(OrderStockCompensation task, String token, String error, boolean manual) {
        boolean exhausted = manual || task.getAttempts() >= maxAttempts;
        String status = exhausted ? OrderStockCompensation.MANUAL_REVIEW : OrderStockCompensation.PENDING;
        long delay = retryBaseSeconds;
        for (int i = 1; i < task.getAttempts() && delay < retryMaxSeconds; i++) {
            delay = Math.min(delay * 2, retryMaxSeconds);
        }
        String detail = error.length() > 1000 ? error.substring(0, 1000) : error;
        int changed = tasks.fail(task.getOrderId(), token, status, detail, (int) delay);
        if (changed == 1 && exhausted) {
            log.error("STOCK_COMPENSATION_MANUAL_REVIEW: orderId={}, attempts={}, reason={}",
                    task.getOrderId(), task.getAttempts(), detail);
        }
    }
}
