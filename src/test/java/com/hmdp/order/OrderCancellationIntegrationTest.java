package com.hmdp.order;

import com.hmdp.entity.OrderStockCompensation;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.OrderCancellationResult;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OrderCancellationIntegrationTest extends OrderIntegrationSupport {
    void createExpired() {
        service.createOrderFromMQ(reservedMessage());
        jdbc.update("UPDATE tb_voucher_order SET create_time=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 16 MINUTE) WHERE id=?", orderId);
    }

    void pending() {
        createExpired();
        service.cancelTimeoutOrder(orderId);
        assertEquals(OrderStockCompensation.PENDING, tasks.selectById(orderId).getStatus());
        assertEquals(10, stock());
        assertEquals("9", redisStock());
    }

    String redisStock() { return redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId); }
    void readyAgain() {
        jdbc.update("UPDATE tb_order_stock_compensation SET next_retry_at=CURRENT_TIMESTAMP(3) WHERE order_id=?", orderId);
    }
    void expireLease() {
        jdbc.update("UPDATE tb_order_stock_compensation SET lease_until=DATE_SUB(CURRENT_TIMESTAMP(3),INTERVAL 1 SECOND) WHERE order_id=?", orderId);
    }
    void assertNotCancelled() {
        assertEquals(ORDER_STATUS_UNPAID, orders.selectById(orderId).getStatus());
        assertEquals(9, stock());
        assertNull(tasks.selectById(orderId));
        assertEquals("9", redisStock());
    }
    void assertDone() {
        assertEquals(ORDER_STATUS_CANCELLED, orders.selectById(orderId).getStatus());
        assertEquals(10, stock());
        assertEquals("10", redisStock());
        assertEquals(OrderStockCompensation.DONE, tasks.selectById(orderId).getStatus());
        assertEquals(-1L, redis.getExpire(SECKILL_STOCK_RETURNED_KEY + orderId));
    }

    @Test void cancellationAndTaskCommitBeforeAnyRedisRefund() {
        pending();
        service.cancelTimeoutOrder(orderId);
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM tb_order_stock_compensation WHERE order_id=?", Integer.class, orderId));
        verify(redisReturns, never()).returnStock(any());
        worker.process(orderId);
        worker.process(orderId);
        service.cancelTimeoutOrder(orderId);
        assertDone();
        assertEquals(1, tasks.selectById(orderId).getAttempts());
        assertEquals(String.valueOf(orderId), redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId)));
        assertTrue(redis.opsForSet().isMember(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId)));
    }

    @Test void mysqlStockFailureRollsBackCancellation() {
        createExpired();
        doThrow(new IllegalStateException("injected stock failure")).when(vouchers).restoreStock(voucherId);
        assertThrows(IllegalStateException.class, () -> service.cancelTimeoutOrder(orderId));
        assertNotCancelled();
    }

    @Test void missingVoucherRowCannotCommitCancellation() {
        createExpired();
        doReturn(0).when(vouchers).restoreStock(voucherId);
        assertThrows(IllegalStateException.class, () -> service.cancelTimeoutOrder(orderId));
        assertNotCancelled();
    }

    @Test void taskInsertFailureRollsBackStatusAndMysqlStock() {
        createExpired();
        doThrow(new IllegalStateException("task insert failed")).when(tasks).insert(any(OrderStockCompensation.class));
        assertThrows(IllegalStateException.class, () -> service.cancelTimeoutOrder(orderId));
        assertNotCancelled();
    }

    @Test void zeroTaskInsertCannotCommitCancellation() {
        createExpired();
        doReturn(0).when(tasks).insert(any(OrderStockCompensation.class));
        assertThrows(IllegalStateException.class, () -> service.cancelTimeoutOrder(orderId));
        assertNotCancelled();
    }

    @Test void exceptionAfterActualTaskInsertRollsBackAllThreeWrites() {
        createExpired();
        doAnswer(invocation -> {
            OrderStockCompensation task = invocation.getArgument(0);
            jdbc.update("INSERT INTO tb_order_stock_compensation(order_id,voucher_id,user_id) VALUES (?,?,?)",
                    task.getOrderId(), task.getVoucherId(), task.getUserId());
            throw new IllegalStateException("failed after task insert");
        }).when(tasks).insert(any(OrderStockCompensation.class));
        assertThrows(IllegalStateException.class, () -> service.cancelTimeoutOrder(orderId));
        assertNotCancelled();
    }

    @Test void databaseDeadlineControlsEarlyAndDueDelivery() {
        service.createOrderFromMQ(reservedMessage());
        jdbc.update("UPDATE tb_voucher_order SET create_time=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 14 MINUTE) WHERE id=?", orderId);
        assertEquals(OrderCancellationResult.Status.NOT_DUE, cancellation.cancelExpired(orderId).getStatus());
        assertNotCancelled();
        jdbc.update("UPDATE tb_voucher_order SET create_time=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 15 MINUTE) WHERE id=?", orderId);
        assertEquals(OrderCancellationResult.Status.CANCELLED, cancellation.cancelExpired(orderId).getStatus());
        assertEquals(10, stock());
    }

    @Test void creationTimestampComesFromMysqlInsteadOfMessage() {
        VoucherOrder order = reservedMessage();
        order.setCreateTime(LocalDateTime.of(2000, 1, 1, 0, 0));
        service.createOrderFromMQ(order);
        long age = jdbc.queryForObject("SELECT TIMESTAMPDIFF(SECOND,create_time,CURRENT_TIMESTAMP) FROM tb_voucher_order WHERE id=?", Long.class, orderId);
        assertTrue(age >= 0 && age < 5);
        assertEquals(OrderCancellationResult.Status.NOT_DUE, cancellation.cancelExpired(orderId).getStatus());
    }

    @Test void paidOrderNeverRefunds() {
        createExpired();
        service.payOrder(orderId);
        service.cancelTimeoutOrder(orderId);
        assertEquals(ORDER_STATUS_PAID, orders.selectById(orderId).getStatus());
        assertEquals(9, stock());
        assertNull(tasks.selectById(orderId));
        assertEquals("9", redisStock());
    }

    @Test void simultaneousPaymentAndCancellationProduceOneTerminalState() throws Exception {
        createExpired();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> cancel = pool.submit(() -> { start.await(); service.cancelTimeoutOrder(orderId); return null; });
            Future<?> pay = pool.submit(() -> { start.await(); login(userId); try { service.payOrder(orderId); } finally { com.hmdp.utils.UserHolder.removeUser(); } return null; });
            start.countDown();
            cancel.get(10, TimeUnit.SECONDS); pay.get(10, TimeUnit.SECONDS);
            if (orders.selectById(orderId).getStatus() == ORDER_STATUS_CANCELLED) {
                worker.process(orderId); assertDone();
            } else {
                assertEquals(ORDER_STATUS_PAID, orders.selectById(orderId).getStatus());
                assertEquals(9, stock()); assertNull(tasks.selectById(orderId));
            }
        } finally { pool.shutdownNow(); }
    }

    @Test void concurrentCancellationsCreateOnlyOneRefundObligation() throws Exception {
        createExpired();
        parallel(() -> service.cancelTimeoutOrder(orderId));
        assertEquals(10, stock());
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM tb_order_stock_compensation WHERE order_id=?", Integer.class, orderId));
        worker.process(orderId); assertDone();
    }

    @Test void multipleWorkersOnlyClaimAndRefundOnce() throws Exception {
        pending();
        parallel(() -> worker.process(orderId));
        assertDone();
        assertEquals(1, tasks.selectById(orderId).getAttempts());
    }

    void parallel(Runnable action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> a = pool.submit(() -> { start.await(); action.run(); return null; });
            Future<?> b = pool.submit(() -> { start.await(); action.run(); return null; });
            start.countDown(); a.get(10, TimeUnit.SECONDS); b.get(10, TimeUnit.SECONDS);
        } finally { pool.shutdownNow(); }
    }

    @Test void lostRedisResponseRetriesWithoutDoubleRefund() {
        pending();
        doAnswer(invocation -> {
            invocation.callRealMethod();
            throw new DataAccessResourceFailureException("Redis response lost after execution");
        }).when(redisReturns).returnStock(any());
        worker.process(orderId);
        assertEquals("10", redisStock());
        assertEquals(OrderStockCompensation.PENDING, tasks.selectById(orderId).getStatus());
        reset(redisReturns); readyAgain(); worker.process(orderId);
        assertDone();
        assertEquals(2, tasks.selectById(orderId).getAttempts());
    }

    @Test void failureToWriteDoneRetriesOnlyRedisIdempotencyCheck() {
        pending();
        doThrow(new DataAccessResourceFailureException("DB unavailable before DONE")).when(tasks).complete(eq(orderId), anyString());
        worker.process(orderId);
        assertEquals("10", redisStock());
        assertEquals(OrderStockCompensation.PENDING, tasks.selectById(orderId).getStatus());
        reset(tasks); readyAgain(); worker.process(orderId);
        assertDone();
    }

    @Test void lostDoneAcknowledgementCannotRegressCompletedTask() {
        pending();
        doAnswer(invocation -> {
            jdbc.update("UPDATE tb_order_stock_compensation SET status='DONE',lease_token=NULL,lease_until=NULL WHERE order_id=?", orderId);
            throw new DataAccessResourceFailureException("DONE committed, response lost");
        }).when(tasks).complete(eq(orderId), anyString());
        worker.process(orderId); assertDone();
    }

    @Test void workerDeathBeforeRedisIsRecoveredAfterLeaseExpiry() {
        pending();
        assertEquals(1, tasks.claim(orderId, "abandoned", 60, 10));
        worker.process(orderId);
        assertEquals("9", redisStock());
        expireLease(); worker.process(orderId);
        assertDone(); assertEquals(2, tasks.selectById(orderId).getAttempts());
    }

    @Test void workerDeathAfterRedisIsRecoveredAfterLeaseExpiry() {
        pending();
        assertEquals(1, tasks.claim(orderId, "abandoned", 60, 10));
        assertEquals(0L, redisReturns.returnStock(tasks.selectById(orderId)));
        expireLease(); worker.process(orderId);
        assertDone(); assertEquals(2, tasks.selectById(orderId).getAttempts());
    }

    @Test void staleLeaseCannotCompleteOrOverwriteNewOwner() {
        pending();
        tasks.claim(orderId, "old", 60, 10);
        OrderStockCompensation old = tasks.selectById(orderId);
        expireLease(); tasks.claim(orderId, "new", 60, 10);
        assertEquals(0L, redisReturns.returnStock(old));
        assertEquals(0, tasks.complete(orderId, "old"));
        assertEquals(0, tasks.fail(orderId, "old", OrderStockCompensation.MANUAL_REVIEW, "stale", 1));
        assertEquals(1L, redisReturns.returnStock(tasks.selectById(orderId)));
        assertEquals(1, tasks.complete(orderId, "new"));
        assertEquals(0, tasks.fail(orderId, "old", OrderStockCompensation.PENDING, "late failure", 1));
        assertDone();
    }

    @Test void boundedRetriesBackOffAndRetainManualReviewRecord() {
        pending(); ReflectionTestUtils.setField(worker, "maxAttempts", 3);
        doThrow(new DataAccessResourceFailureException("Redis unavailable")).when(redisReturns).returnStock(any());
        for (int attempt = 1; attempt <= 3; attempt++) {
            readyAgain(); worker.process(orderId);
            OrderStockCompensation task = tasks.selectById(orderId);
            assertEquals(attempt, task.getAttempts());
            long delay = jdbc.queryForObject("SELECT TIMESTAMPDIFF(SECOND,CURRENT_TIMESTAMP(3),next_retry_at) FROM tb_order_stock_compensation WHERE order_id=?", Long.class, orderId);
            assertTrue(delay > 0);
            assertEquals(attempt < 3 ? OrderStockCompensation.PENDING : OrderStockCompensation.MANUAL_REVIEW, task.getStatus());
            worker.process(orderId); // Not due or manual; must not consume another attempt.
            assertEquals(attempt, tasks.selectById(orderId).getAttempts());
        }
        assertEquals("9", redisStock());
        assertNotNull(tasks.selectById(orderId).getLastError());
    }

    @Test void lastAttemptWorkerDeathEventuallyBecomesManualReview() {
        pending(); ReflectionTestUtils.setField(worker, "maxAttempts", 1);
        tasks.claim(orderId, "last", 60, 1); expireLease(); worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals("9", redisStock());
    }

    @Test void missingStockKeyRequiresReviewInsteadOfCreatingStock() {
        pending(); redis.delete(SECKILL_STOCK_KEY + voucherId); worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertNull(redisStock());
        assertFalse(redis.hasKey(SECKILL_STOCK_RETURNED_KEY + orderId));
    }

    @Test void mismatchedOwnerRequiresReviewWithoutChangingStock() {
        pending(); redis.opsForHash().put(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId), "other-order");
        worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals("9", redisStock());
        assertFalse(redis.hasKey(SECKILL_STOCK_RETURNED_KEY + orderId));
    }

    @Test void wrongMarkerTypeIsDetectedBeforeAnyIncrement() {
        pending(); redis.opsForSet().add(SECKILL_STOCK_RETURNED_KEY + orderId, "bad-type"); worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals("9", redisStock());
    }

    @Test void expiringOwnershipIsNotAcceptedAsLongTermProof() {
        pending(); redis.expire(SECKILL_CLAIM_OWNER_KEY + voucherId, 1, TimeUnit.HOURS); worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals("9", redisStock());
    }

    @Test void expiringReturnMarkerIsNotAcceptedForReplay() {
        pending();
        String value = voucherId + ":" + userId + ":" + orderId;
        redis.opsForValue().set(SECKILL_STOCK_RETURNED_KEY + orderId, value, 1, TimeUnit.HOURS);
        worker.process(orderId);
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals("9", redisStock());
    }

    @Test void consumerCannotCreateWithoutReservation() {
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(message()));
        assertEquals("10", redisStock()); assertEquals(0, count());
    }

    @Test void ownershipSurvivesTransactionRecordLossAndRefundMarkerHasNoTtl() {
        createExpired();
        assertEquals(-1L, redis.getExpire(SECKILL_CLAIM_OWNER_KEY + voucherId));
        assertEquals(-1L, redis.getExpire(SECKILL_ORDER_KEY + voucherId));
        redis.delete(SECKILL_TXN_KEY + orderId);
        service.cancelTimeoutOrder(orderId); worker.process(orderId); assertDone();
        assertEquals(1L, redisReturns.returnStock(tasks.selectById(orderId)));
        assertDone();
    }

    @Test void ownershipAndMarkersRemainAfterCancellation() {
        pending();
        assertEquals(-1L, redis.getExpire(SECKILL_CLAIM_OWNER_KEY + voucherId));
        worker.process(orderId); assertDone();
        VoucherOrder repeat = reservedMessage(); repeat.setId(orderId + 1);
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(repeat));
        assertEquals("10", redisStock());
    }

    @Test void malformedOwnershipCannotDeductStock() {
        redis.opsForValue().set(SECKILL_CLAIM_OWNER_KEY + voucherId, "wrong-type");
        assertEquals(-1, reservations.reserve(message()));
        assertThrows(RuntimeException.class, () -> service.createOrderFromMQ(message()));
        assertEquals("10", redisStock()); assertEquals(10, stock()); assertEquals(0, count());
        assertFalse(redis.opsForSet().isMember(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId)));
    }

    @Test void oldCancelledOrderIsNotBlindlyBackfilledOrRefunded() {
        createExpired(); jdbc.update("UPDATE tb_voucher_order SET status=4 WHERE id=?", orderId);
        assertEquals(OrderCancellationResult.Status.LEGACY_REVIEW, cancellation.cancelExpired(orderId).getStatus());
        service.cancelTimeoutOrder(orderId); worker.process(orderId);
        assertEquals(9, stock()); assertEquals("9", redisStock()); assertNull(tasks.selectById(orderId));
    }

    @Test void auditCannotOverwriteStocksEvenWithoutPendingTasks() {
        service.createOrderFromMQ(reservedMessage());
        jdbc.update("UPDATE tb_seckill_voucher SET stock=77,end_time=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 5 MINUTE) WHERE voucher_id=?", voucherId);
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "88");
        reconcile.reconcile();
        assertEquals(77, stock()); assertEquals("88", redisStock());
    }

    @Test void badTaskDoesNotBlockAnotherTaskInBatch() {
        pending();
        VoucherOrder second = reservedMessage(); second.setId(orderId + 1); second.setUserId(userId + 1);
        reserve(second);
        service.createOrderFromMQ(second);
        jdbc.update("UPDATE tb_voucher_order SET create_time=DATE_SUB(CURRENT_TIMESTAMP,INTERVAL 16 MINUTE) WHERE id=?", second.getId());
        service.cancelTimeoutOrder(second.getId());
        redis.opsForHash().delete(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId));
        worker.runBatch();
        assertEquals(OrderStockCompensation.MANUAL_REVIEW, tasks.selectById(orderId).getStatus());
        assertEquals(OrderStockCompensation.DONE, tasks.selectById(second.getId()).getStatus());
        assertEquals("9", redisStock());
    }

    @Test void deathAfterCancellationCasRollsBackAndRecovers() throws Exception { crashCancellation("cancel-after-cas", false); }
    @Test void deathAfterMysqlRefundRollsBackAndRecovers() throws Exception { crashCancellation("cancel-after-stock", false); }
    @Test void deathAfterTaskInsertRollsBackAndRecovers() throws Exception { crashCancellation("cancel-after-task", false); }
    @Test void deathAfterCancellationCommitLeavesRecoverableTask() throws Exception { crashCancellation("cancel-after-commit", true); }

    void crashCancellation(String stage, boolean committed) throws Exception {
        createExpired();
        ProcessBuilder builder = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                System.getProperty("java.class.path"), CrashBeforeCommitProbe.class.getName(), stage,
                String.valueOf(voucherId), String.valueOf(userId), String.valueOf(orderId));
        try (Connection connection = jdbc.getDataSource().getConnection()) {
            builder.environment().put("HMDP_TEST_JDBC_URL", connection.getMetaData().getURL());
        }
        Process process = builder.redirectErrorStream(true).redirectOutput(new File("target/crash-probe-" + stage + ".log")).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS)); assertEquals(23, process.exitValue());
            if (committed) {
                assertEquals(10, stock());
                assertEquals(OrderStockCompensation.PENDING, tasks.selectById(orderId).getStatus());
            } else { assertNotCancelled(); }
            service.cancelTimeoutOrder(orderId); worker.process(orderId); assertDone();
        } finally { process.destroyForcibly(); }
    }
}
