package com.hmdp.order;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.*;
import com.hmdp.utils.*;
import org.junit.jupiter.api.*;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.io.File;
import java.sql.Connection;
import java.util.Arrays;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.delegatesTo;

class OrderConsumptionIntegrationTest extends OrderIntegrationSupport {
    @Test void committedMessageCanCreateAfterMarkerExpiryAndReplayWithoutRedis() {
        VoucherOrder order = reservedMessage();
        redis.delete(SECKILL_TXN_KEY + orderId);
        service.createOrderFromMQ(order); // Ownership, not the short-lived marker, authorizes first consumption.
        com.hmdp.service.SeckillReservationService unavailable = mock(com.hmdp.service.SeckillReservationService.class);
        org.springframework.test.util.ReflectionTestUtils.setField(service, "reservations", unavailable);
        try {
            service.createOrderFromMQ(order); // DB commit + lost ACK: duplicate bypasses Redis.
            verifyNoInteractions(unavailable);
            assertEquals(1, count()); assertEquals(9, stock());
            assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        } finally { org.springframework.test.util.ReflectionTestUtils.setField(service, "reservations", reservations); }
    }

    @Test
    void originalOrderCommittingBetweenIdentityReadsIsStillIdempotent() {
        VoucherOrder original = reservedMessage();
        service.createOrderFromMQ(original);
        // Reproduce the first read occurring before commit and the purchase lookup after commit.
        doReturn(null).when(orders).selectById(orderId);
        assertEquals(com.hmdp.service.OrderPersistenceResult.Status.ALREADY_CREATED,
                persistence.findExisting(original).getStatus());
        assertDoesNotThrow(() -> service.createOrderFromMQ(original));
        assertEquals(1, count());
        assertEquals(9, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test
    void sameMessageRetriesDoNotDeductAgainAndRepeatPostCommitWork() throws Exception {
        service.createOrderFromMQ(reservedMessage());
        service.createOrderFromMQ(reservedMessage());
        assertEquals(9, stock());
        assertEquals(1, count());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertNotNull(orders.selectById(orderId));
        verify(producer, times(2)).sendOrderTimeout(argThat(o -> o.getId() == orderId));
    }

    @Test
    void failedInsertRollsBackStockAndOriginalMessageCanRecover() {
        doThrow(new IllegalStateException("injected before insert")).when(orders).insert(any(VoucherOrder.class));
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(reservedMessage()));
        assertEquals(10, stock());
        assertEquals(0, count());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        reset(orders);
        service.createOrderFromMQ(reservedMessage());
        assertEquals(9, stock());
        assertEquals(1, count());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test
    void zeroAffectedInsertIsFailureAndRollsBack() {
        doReturn(0).when(orders).insert(any(VoucherOrder.class));
        assertThrows(IllegalStateException.class, () -> persistence.createOrder(message()));
        assertEquals(10, stock());
        assertEquals(0, count());
    }

    int insertUsingSameConnection(VoucherOrder order) {
        assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
        return jdbc.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,create_time) VALUES (?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP))",
                order.getId(), order.getUserId(), order.getVoucherId(), order.getStatus(), order.getCreateTime());
    }

    @Test
    void failureAfterActualInsertRollsBackBothWrites() {
        doAnswer(invocation -> {
            insertUsingSameConnection(invocation.getArgument(0));
            throw new IllegalStateException("injected after insert, before commit");
        }).when(orders).insert(any(VoucherOrder.class));
        assertThrows(IllegalStateException.class, () -> persistence.createOrder(message()));
        assertEquals(10, stock());
        assertEquals(0, count());
    }

    @Test
    void processDeathAfterStockWriteRollsBackAndReplayRecovers() throws Exception {
        crashAndReplay("after-stock", false);
    }

    @Test
    void processDeathAfterOrderInsertRollsBackAndReplayRecovers() throws Exception {
        crashAndReplay("after-order", false);
    }

    @Test
    void processDeathAfterCommitDoesNotDuplicateOnReplay() throws Exception {
        crashAndReplay("after-commit", true);
    }

    void crashAndReplay(String stage, boolean committed) throws Exception {
        assertEquals(0, reservations.reserve(message()));
        File log = new File("target/crash-probe-" + stage + ".log");
        ProcessBuilder builder = new ProcessBuilder(System.getProperty("java.home") + "/bin/java",
                "-cp", System.getProperty("java.class.path"), CrashBeforeCommitProbe.class.getName(),
                stage, String.valueOf(voucherId), String.valueOf(userId), String.valueOf(orderId));
        try (Connection connection = jdbc.getDataSource().getConnection()) {
            builder.environment().put("HMDP_TEST_JDBC_URL", connection.getMetaData().getURL());
        }
        Process process = builder.redirectErrorStream(true).redirectOutput(log).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "child did not terminate");
            assertEquals(23, process.exitValue(), "see " + log);
            assertEquals(committed ? 9 : 10, stock());
            assertEquals(committed ? 1 : 0, count());
            service.createOrderFromMQ(reservedMessage());
            assertEquals(9, stock());
            assertEquals(1, count());
            assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertNotNull(orders.selectById(orderId));
        } finally { process.destroyForcibly(); }
    }

    @Test
    void databaseTransactionCommitsBeforeTimeoutSend() throws Exception {
        doAnswer(invocation -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            assertNotNull(orders.selectById(orderId));
            return null;
        }).when(producer).sendOrderTimeout(any());
        service.createOrderFromMQ(reservedMessage());
        assertEquals(9, stock());
    }

    @Test
    void failedTimeoutSendAfterCommitIsRecoveredWithoutNewOrder() throws Exception {
        doThrow(new IllegalStateException("send failed")).doNothing().when(producer).sendOrderTimeout(any());
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(reservedMessage()));
        assertEquals(9, stock());
        assertEquals(1, count());
        LocalDateTime created = orders.selectById(orderId).getCreateTime();
        assertNotNull(orders.selectById(orderId));
        service.createOrderFromMQ(reservedMessage());
        assertEquals(9, stock());
        assertEquals(created, orders.selectById(orderId).getCreateTime());
        assertNotNull(orders.selectById(orderId));
        verify(producer, times(2)).sendOrderTimeout(argThat(o -> o.getCreateTime().equals(created)));
    }

    @Test
    void anotherOrderCannotTakeOverFailedReservation() {
        doThrow(new IllegalStateException("db unavailable")).when(orders).insert(any(VoucherOrder.class));
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(reservedMessage()));
        reset(orders);
        VoucherOrder other = reservedMessage();
        other.setId(orderId + 1);
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(other));
        assertEquals(0, count());
        assertEquals(String.valueOf(orderId), redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId)));
        service.createOrderFromMQ(reservedMessage());
        assertNotNull(orders.selectById(orderId));
        assertEquals(9, stock());
    }

    @Test
    void duplicatePurchaseDoesNotMarkNonexistentOrderSuccessful() {
        service.createOrderFromMQ(reservedMessage());
        VoucherOrder other = reservedMessage();
        other.setId(orderId + 1);
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(other));
        assertNull(orders.selectById(orderId + 1));
        assertEquals(9, stock());
    }

    @Test
    void stockRejectedRetainsReservationAndThrowsEveryAttempt() {
        jdbc.update("UPDATE tb_seckill_voucher SET stock=0 WHERE voucher_id=?", voucherId);
        reserve(message());
        for (int i = 0; i < 2; i++) {
            assertThrows(StockRejectedException.class, () -> service.createOrderFromMQ(message()));
        }
        assertEquals(0, count()); assertEquals(0, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertTrue(reservations.hasSuccessMarker(orderId));
        assertEquals(orderId, reservations.findOriginalOrderId(voucherId, userId));
    }

    @Test
    void unownedLegacyClaimRequiresManualReview() {
        redis.opsForSet().add(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId));
        assertThrows(com.hmdp.service.SeckillUnavailableException.class, () -> service.createOrderFromMQ(message()));
        assertEquals(10, stock());
        assertEquals(0, count());
        assertNull(redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId)));
    }

    @Test
    void concurrentConsumerReplaysCreateOnlyOneOrderWithoutLock() throws Exception {
        VoucherOrder order = reservedMessage();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        java.util.List<Future<?>> results = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) results.add(pool.submit(() -> {
                start.await(); service.createOrderFromMQ(order); return null;
            }));
            start.countDown();
            for (Future<?> result : results) result.get(10, TimeUnit.SECONDS);
            assertEquals(1, count()); assertEquals(9, stock());
            assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        } finally { pool.shutdownNow(); }
    }

    @Test
    void primaryKeyCollisionWithDifferentBusinessDataIsNotIdempotentSuccess() {
        persistence.createOrder(message());
        VoucherOrder collision = reservedMessage();
        collision.setUserId(userId + 1);
        assertThrows(IllegalStateException.class, () -> service.createOrderFromMQ(collision));
        assertEquals(9, stock());
        assertEquals(1, count());
    }

    @Test
    void concurrentDifferentOrdersRollbackUniqueKeyLoserWithoutDistributedLock() throws Exception {
        racePersistence(false);
    }

    @Test
    void concurrentSameOrderRollbackPrimaryKeyLoserWithoutDistributedLock() throws Exception {
        racePersistence(true);
    }

    void racePersistence(boolean sameId) throws Exception {
        // Force both transactions past their initial reads before the first stock update.
        CyclicBarrier barrier = new CyclicBarrier(2);
        doAnswer(invocation -> {
            barrier.await(5, TimeUnit.SECONDS);
            return jdbc.update("UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=? AND stock>0", voucherId);
        }).when(vouchers).decreaseStock(voucherId);
        VoucherOrder second = message();
        if (!sameId) second.setId(orderId + 1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = pool.submit(() -> persistence.createOrder(message()));
            Future<?> other = pool.submit(() -> persistence.createOrder(second));
            int committed = 0, conflicts = 0;
            for (Future<?> future : Arrays.asList(first, other)) {
                try { future.get(10, TimeUnit.SECONDS); committed++; }
                catch (ExecutionException e) {
                    assertTrue(e.getCause() instanceof DuplicateKeyException, String.valueOf(e.getCause()));
                    conflicts++;
                }
            }
            assertEquals(1, committed);
            assertEquals(1, conflicts);
            assertEquals(9, stock());
            assertEquals(1, count());
        } finally { pool.shutdownNow(); }
    }

    @Test
    void earlyTimeoutIsRescheduledButCannotCancelOrder() throws Exception {
        persistence.createOrder(message());
        service.cancelTimeoutOrder(orderId);
        assertEquals(ORDER_STATUS_UNPAID, orders.selectById(orderId).getStatus());
        assertEquals(9, stock());
        verify(producer).sendOrderTimeout(argThat(o -> o.getId() == orderId));
    }

    @Test
    void expiredAndDuplicateTimeoutsUseCasAndReturnStockOnce() {
        service.createOrderFromMQ(reservedMessage());
        jdbc.update("UPDATE tb_voucher_order SET create_time=? WHERE id=?", LocalDateTime.now().minusMinutes(16), orderId);
        service.cancelTimeoutOrder(orderId);
        service.cancelTimeoutOrder(orderId);
        assertEquals(ORDER_STATUS_CANCELLED, orders.selectById(orderId).getStatus());
        assertEquals(10, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        worker.process(orderId);
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test
    void reconciliationDoesNotRepublishOrReleaseUnresolvedClaim() throws Exception {
        jdbc.update("UPDATE tb_seckill_voucher SET stock=0,end_time=? WHERE voucher_id=?", LocalDateTime.now().minusMinutes(5), voucherId);
        assertThrows(StockRejectedException.class, () -> service.createOrderFromMQ(reservedMessage()));
        reconcile.reconcile();
        assertEquals(0, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        verify(producer, never()).sendOrderCreateInTransaction(any(), any());
        assertEquals(0, count());
    }
}
