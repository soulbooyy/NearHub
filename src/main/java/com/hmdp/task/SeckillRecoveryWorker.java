package com.hmdp.task;

import com.hmdp.entity.SeckillRecoveryTask;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillRecoveryTaskMapper;
import com.hmdp.mq.RocketMQProducer;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Objects;
import java.util.UUID;

/** Dedicated missing-order recovery; same lease/backoff pattern as stock compensation, no cross-system transaction. */
@Slf4j
@Component
@RequiredArgsConstructor
public class SeckillRecoveryWorker {
    private final SeckillRecoveryTaskMapper tasks;
    private final SeckillReservationService reservations;
    private final VoucherOrderPersistenceService persistence;
    private final RocketMQProducer producer;
    @Value("${seckill.recovery.batch-size:100}") private int batchSize;
    @Value("${seckill.recovery.max-attempts:5}") private int maxAttempts;
    @Value("${seckill.recovery.lease-seconds:60}") private int leaseSeconds;
    @Value("${seckill.recovery.retry-base-seconds:30}") private int retryBaseSeconds;
    @Value("${seckill.recovery.retry-max-seconds:600}") private int retryMaxSeconds;
    @Value("${seckill.recovery.discovery-grace-ms:60000}") private long discoveryGraceMs;

    @PostConstruct public void validateConfiguration() {
        if (batchSize < 1 || maxAttempts < 1 || leaseSeconds < 1 || retryBaseSeconds < 1
                || retryMaxSeconds < retryBaseSeconds || discoveryGraceMs < 1) {
            throw new IllegalArgumentException("Invalid order recovery configuration");
        }
    }

    /** Invoked under the existing reconcile lock. Incomplete/corrupt evidence is never registered for automatic resend. */
    public void discover() {
        reservations.scanTransactions((order, record) -> {
            if ("1".equals(record.get("writePending")) || "1".equals(record.get("review"))) {
                log.error("ORDER_RECOVERY_MANUAL_REVIEW: diagnostic guard, orderId={}", order.getId()); return;
            }
            if ("PREPARED".equals(record.get("state"))) { reservations.check(order); return; }
            if (!"RESERVED".equals(record.get("state"))) return;
            Object timestamp = record.get("reservedAt");
            if (timestamp == null || System.currentTimeMillis() - Long.parseLong(timestamp.toString()) < discoveryGraceMs) return;
            if (reservations.check(order) != SeckillReservationService.RESERVED) return;
            OrderPersistenceResult existing = persistence.findExisting(order);
            if (existing != null) {
                if (existing.getStatus() == OrderPersistenceResult.Status.DUPLICATE_PURCHASE)
                    log.error("ORDER_RECOVERY_MANUAL_REVIEW: conflicting purchase, orderId={}", order.getId());
                return;
            }
            tasks.register(order); // Duplicate discovery never resets attempts, identity, or terminal status.
            SeckillRecoveryTask task = tasks.selectById(order.getId());
            if (task == null || !Objects.equals(task.getUserId(), order.getUserId())
                    || !Objects.equals(task.getVoucherId(), order.getVoucherId())) {
                log.error("ORDER_RECOVERY_MANUAL_REVIEW: task identity conflict, orderId={}", order.getId());
            }
        });
    }

    @Scheduled(fixedDelayString = "${seckill.recovery.poll-ms:5000}")
    public void runBatch() {
        for (Long id : tasks.findDueIds(batchSize)) {
            try { process(id); }
            catch (Exception e) { log.error("ORDER_RECOVERY_RETRY: orderId={}", id, e); }
        }
    }

    public void process(Long orderId) {
        String token = UUID.randomUUID().toString();
        if (tasks.claim(orderId, token, leaseSeconds) != 1) return;
        SeckillRecoveryTask task = tasks.selectById(orderId);
        if (task == null || !token.equals(task.getLeaseToken())) return;
        boolean attempted = false;
        try {
            VoucherOrder order = new VoucherOrder().setId(task.getOrderId())
                    .setUserId(task.getUserId()).setVoucherId(task.getVoucherId());
            SeckillReservationService.validateIdentity(order);
            // Always confirm the final send, even when its attempt exhausted the resend budget.
            OrderPersistenceResult existing;
            try { existing = persistence.findExisting(order); }
            catch (IllegalStateException conflict) { finish(task, token, conflict.getMessage(), true); return; }
            if (existing != null) {
                if (existing.getStatus() == OrderPersistenceResult.Status.ALREADY_CREATED) tasks.complete(orderId, token);
                else finish(task, token, "Another order owns this purchase", true);
                return;
            }
            if (task.getAttempts() >= maxAttempts) {
                finish(task, token, "Resend budget exhausted; MySQL order still absent", true); return;
            }
            if (tasks.beginAttempt(orderId, token, maxAttempts) != 1) return;
            attempted = true;
            task.setAttempts(task.getAttempts() + 1);
            if (reservations.check(order) != SeckillReservationService.RESERVED) {
                finish(task, token, "Missing or conflicting reservation evidence", true); return;
            }
            SeckillTxContext context = new SeckillTxContext(order);
            // No PREPARE, no new ID. RESERVED callback is idempotent and never decrements stock again.
            SendResult sent = producer.sendOrderCreateInTransaction(order, context);
            if (sent == null || sent.getSendStatus() != SendStatus.SEND_OK
                    || context.getLuaResult() != SeckillReservationService.RESERVED) {
                finish(task, token, "Transaction send outcome uncertain", false); return;
            }
            finish(task, token, "Sent; waiting for MySQL confirmation", false);
        } catch (IllegalArgumentException e) {
            finish(task, token, e.getClass().getSimpleName() + ": " + e.getMessage(), true);
        } catch (Exception e) {
            // Lost response is retried using the same ID. DB errors before send also consume a bounded attempt.
            if (!attempted && task.getAttempts() < maxAttempts && tasks.beginAttempt(orderId, token, maxAttempts) == 1)
                task.setAttempts(task.getAttempts() + 1);
            finish(task, token, e.getClass().getSimpleName() + ": " + e.getMessage(), task.getAttempts() >= maxAttempts && !attempted);
        }
    }

    private void finish(SeckillRecoveryTask task, String token, String error, boolean manual) {
        long delay = retryBaseSeconds;
        for (int i = 1; i < task.getAttempts() && delay < retryMaxSeconds; i++) delay = Math.min(delay * 2, retryMaxSeconds);
        String detail = error.length() > 1000 ? error.substring(0, 1000) : error;
        int changed = tasks.fail(task.getOrderId(), token,
                manual ? SeckillRecoveryTask.MANUAL_REVIEW : SeckillRecoveryTask.PENDING, detail, (int) delay);
        if (changed == 1 && manual) log.error("ORDER_RECOVERY_MANUAL_REVIEW: orderId={}, reason={}", task.getOrderId(), detail);
    }
}
