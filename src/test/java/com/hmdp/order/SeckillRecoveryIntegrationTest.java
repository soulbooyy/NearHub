package com.hmdp.order;

import com.hmdp.entity.SeckillRecoveryTask;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.SeckillReservationService;
import com.hmdp.service.SeckillUnavailableException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real Redis/MySQL; transport is deliberately mocked, no real Broker fault injection. */
class SeckillRecoveryIntegrationTest extends OrderIntegrationSupport {
    @AfterEach void resetRecovery() {
        ReflectionTestUtils.setField(recovery, "reservations", reservations);
        ReflectionTestUtils.setField(recovery, "maxAttempts", 5);
    }

    private VoucherOrder discoverMissing() {
        VoucherOrder order = reservedMessage();
        redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "reservedAt", "1");
        recovery.discover();
        assertNotNull(recoveryTasks.selectById(orderId));
        return order;
    }
    private void due() {
        jdbc.update("UPDATE tb_seckill_recovery_task SET next_retry_at=DATE_SUB(CURRENT_TIMESTAMP(3),INTERVAL 1 SECOND) WHERE order_id=?", orderId);
    }
    private void successfulTransport() throws Exception {
        when(producer.sendOrderCreateInTransaction(any(), any())).thenAnswer(invocation -> {
            VoucherOrder order = invocation.getArgument(0);
            SeckillTxContext ctx = invocation.getArgument(1);
            assertEquals(orderId, order.getId().longValue());
            ctx.setLuaResult(reservations.reserve(order));
            SendResult sent = new SendResult(); sent.setSendStatus(SendStatus.SEND_OK); return sent;
        });
    }
    private String status() { return recoveryTasks.selectById(orderId).getStatus(); }
    private int attempts() { return recoveryTasks.selectById(orderId).getAttempts(); }

    @Test void resendOriginalIdNeverDeductsTwiceAndOnlyDatabaseConfirmsCompletion() throws Exception {
        VoucherOrder original = discoverMissing(); successfulTransport();
        recovery.process(orderId);
        assertEquals(SeckillRecoveryTask.PENDING, status()); assertEquals(1, attempts());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId)); assertEquals(10, stock());
        service.createOrderFromMQ(original); due(); recovery.process(orderId);
        assertEquals(SeckillRecoveryTask.DONE, status()); assertEquals(1, attempts());
        assertEquals(1, count()); assertEquals(9, stock());
        verify(producer, times(1)).sendOrderCreateInTransaction(any(), any());
    }

    @Test void recentAndUncertainReservationsAreNotAutomaticallyRecovered() {
        reservedMessage(); recovery.discover(); assertNull(recoveryTasks.selectById(orderId));
        redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "reservedAt", "1");
        redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "writePending", "1");
        recovery.discover(); assertNull(recoveryTasks.selectById(orderId));
        redis.delete(SECKILL_TXN_KEY + orderId);
        recovery.discover(); assertNull(recoveryTasks.selectById(orderId));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void duplicateDiscoveryPreservesBudgetAndManualTerminalState() {
        discoverMissing();
        jdbc.update("UPDATE tb_seckill_recovery_task SET attempts=5,status='MANUAL_REVIEW',last_error='review' WHERE order_id=?", orderId);
        recovery.discover(); recovery.process(orderId);
        assertEquals(5, attempts()); assertEquals(SeckillRecoveryTask.MANUAL_REVIEW, status());
        assertEquals("review", recoveryTasks.selectById(orderId).getLastError());
        assertEquals("MANUAL_REVIEW", ((Map<?,?>) service.getSeckillResult(orderId).getData()).get("status"));
        verifyNoInteractions(producer);
    }

    @Test void lastSuccessfulAttemptGetsConfirmationAfterBudgetIsUsed() throws Exception {
        VoucherOrder order = discoverMissing(); successfulTransport();
        ReflectionTestUtils.setField(recovery, "maxAttempts", 1);
        recovery.process(orderId); assertEquals(SeckillRecoveryTask.PENDING, status());
        service.createOrderFromMQ(order); due(); recovery.process(orderId);
        assertEquals(SeckillRecoveryTask.DONE, status()); assertEquals(1, attempts());
    }

    @Test void failedSendsHaveBoundedPersistentBudgetAndBackoff() throws Exception {
        discoverMissing(); ReflectionTestUtils.setField(recovery, "maxAttempts", 2);
        when(producer.sendOrderCreateInTransaction(any(), any())).thenThrow(new MQClientException("unavailable", null));
        recovery.process(orderId);
        assertEquals(1, attempts()); assertEquals(SeckillRecoveryTask.PENDING, status());
        assertTrue(recoveryTasks.selectById(orderId).getNextRetryAt().isAfter(orders.databaseNow()));
        recovery.process(orderId); assertEquals(1, attempts()); // not due
        due(); recovery.process(orderId); assertEquals(2, attempts());
        due(); recovery.process(orderId); assertEquals(SeckillRecoveryTask.MANUAL_REVIEW, status());
        recovery.discover(); recovery.process(orderId); assertEquals(2, attempts());
        verify(producer, times(2)).sendOrderCreateInTransaction(any(), any());
        assertEquals(10, stock()); assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void reservationRedisOutageRetriesWithoutRefundOrSending() {
        discoverMissing(); SeckillReservationService failing = mock(SeckillReservationService.class);
        when(failing.check(any())).thenThrow(new SeckillUnavailableException("injected outage"));
        ReflectionTestUtils.setField(recovery, "reservations", failing);
        recovery.process(orderId);
        assertEquals(1, attempts()); assertEquals(SeckillRecoveryTask.PENDING, status());
        verifyNoInteractions(producer); assertEquals(0, count()); assertEquals(10, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void missingEvidenceRequiresReviewInsteadOfPrepareOrNewReservation() {
        discoverMissing(); redis.delete(SECKILL_TXN_KEY + orderId); recovery.process(orderId);
        assertEquals(SeckillRecoveryTask.MANUAL_REVIEW, status()); verifyNoInteractions(producer);
        assertFalse(Boolean.TRUE.equals(redis.hasKey(SECKILL_TXN_KEY + orderId)));
        assertEquals("MANUAL_REVIEW", ((Map<?,?>) service.getSeckillResultByVoucher(voucherId).getData()).get("status"));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void corruptedTaskIdentityCannotTakeOverReservation() {
        discoverMissing(); jdbc.update("UPDATE tb_seckill_recovery_task SET user_id=user_id+1 WHERE order_id=?", orderId);
        recovery.process(orderId); assertEquals(SeckillRecoveryTask.MANUAL_REVIEW, status());
        verifyNoInteractions(producer); assertEquals(0, count());
        assertEquals(String.valueOf(orderId), redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId)));
    }

    @Test void databaseCommitWithLostAcknowledgementCompletesOnNextPass() throws Exception {
        discoverMissing();
        when(producer.sendOrderCreateInTransaction(any(), any())).thenAnswer(invocation -> {
            service.createOrderFromMQ(invocation.getArgument(0));
            throw new MQClientException("lost send response after delivery", null);
        });
        recovery.process(orderId); assertEquals(1, count()); assertEquals(SeckillRecoveryTask.PENDING, status());
        due(); recovery.process(orderId); assertEquals(SeckillRecoveryTask.DONE, status());
        assertEquals(9, stock()); verify(producer, times(1)).sendOrderCreateInTransaction(any(), any());
    }

    @Test void originalAndRecoveryDeliveriesCanArriveConcurrently() throws Exception {
        VoucherOrder order = discoverMissing();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        when(producer.sendOrderCreateInTransaction(any(), any())).thenAnswer(invocation -> {
            SeckillTxContext ctx = invocation.getArgument(1); ctx.setLuaResult(reservations.reserve(order));
            start.countDown(); service.createOrderFromMQ(order);
            SendResult sent = new SendResult(); sent.setSendStatus(SendStatus.SEND_OK); return sent;
        });
        try {
            Future<?> original = pool.submit(() -> { try { assertTrue(start.await(5, TimeUnit.SECONDS)); service.createOrderFromMQ(order); }
                catch (InterruptedException e) { throw new RuntimeException(e); } });
            Future<?> resend = pool.submit(() -> recovery.process(orderId));
            original.get(10, TimeUnit.SECONDS); resend.get(10, TimeUnit.SECONDS);
        } finally { pool.shutdownNow(); }
        due(); recovery.process(orderId); assertEquals(SeckillRecoveryTask.DONE, status());
        assertEquals(1, count()); assertEquals(9, stock()); assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void concurrentWorkersHaveOnlyOneLiveLease() throws Exception {
        discoverMissing(); successfulTransport();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<Void> job = () -> { start.await(); recovery.process(orderId); return null; };
        try { Future<Void> a = pool.submit(job), b = pool.submit(job); start.countDown(); a.get(10, TimeUnit.SECONDS); b.get(10, TimeUnit.SECONDS); }
        finally { pool.shutdownNow(); }
        assertEquals(1, attempts()); verify(producer, times(1)).sendOrderCreateInTransaction(any(), any());
    }

    @Test void expiredLeaseAllowsRestartButOldTokenCannotFinishOrSpendAttempts() throws Exception {
        discoverMissing(); successfulTransport();
        assertEquals(1, recoveryTasks.claim(orderId, "old", 60));
        assertEquals(1, recoveryTasks.beginAttempt(orderId, "old", 5));
        jdbc.update("UPDATE tb_seckill_recovery_task SET lease_until=DATE_SUB(CURRENT_TIMESTAMP(3),INTERVAL 1 SECOND) WHERE order_id=?", orderId);
        recovery.process(orderId); assertEquals(2, attempts());
        assertEquals(0, recoveryTasks.complete(orderId, "old"));
        assertEquals(0, recoveryTasks.beginAttempt(orderId, "old", 5));
        assertEquals(0, recoveryTasks.fail(orderId, "old", "DONE", "stale", 1));
        assertEquals(SeckillRecoveryTask.PENDING, status());
    }

    @Test void databaseInventoryFailureKeepsReservationAndRecoveryRetries() throws Exception {
        VoucherOrder order = discoverMissing(); successfulTransport();
        jdbc.update("UPDATE tb_seckill_voucher SET stock=0 WHERE voucher_id=?", voucherId);
        recovery.process(orderId); assertThrows(RuntimeException.class, () -> service.createOrderFromMQ(order));
        assertEquals(0, count()); assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        jdbc.update("UPDATE tb_seckill_voucher SET stock=10 WHERE voucher_id=?", voucherId);
        service.createOrderFromMQ(order); due(); recovery.process(orderId);
        assertEquals(SeckillRecoveryTask.DONE, status()); assertEquals(9, stock());
    }

    @Test void committedOrderNeedsNoRedisEvidenceAndNoResend() {
        VoucherOrder order = discoverMissing(); service.createOrderFromMQ(order);
        redis.delete(SECKILL_TXN_KEY + orderId); clearInvocations(producer);
        recovery.process(orderId); assertEquals(SeckillRecoveryTask.DONE, status());
        assertEquals(0, attempts()); verifyNoInteractions(producer);
    }
}
