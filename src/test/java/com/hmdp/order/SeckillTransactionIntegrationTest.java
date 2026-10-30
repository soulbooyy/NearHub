package com.hmdp.order;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.SeckillTransactionListener;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.SeckillReservationService;
import com.hmdp.service.SeckillUnavailableException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real Redis/MySQL; invokes real callbacks directly. These are NOT real Broker fault-injection tests. */
class SeckillTransactionIntegrationTest extends OrderIntegrationSupport {
    final List<String> extraKeys = new ArrayList<>();
    @AfterEach void cleanExtraKeys() { if (!extraKeys.isEmpty()) redis.delete(extraKeys); }

    SeckillTransactionListener listener(SeckillReservationService storage) {
        SeckillTransactionListener listener = new SeckillTransactionListener();
        ReflectionTestUtils.setField(listener, "reservations", storage);
        ReflectionTestUtils.setField(listener, "checkGraceMs", 60000L);
        return listener;
    }
    MessageExt payload(VoucherOrder order) {
        MessageExt message = new MessageExt();
        message.setBody(JSONUtil.toJsonStr(order).getBytes(StandardCharsets.UTF_8));
        message.setBornTimestamp(System.currentTimeMillis() - 120000);
        return message;
    }

    @Test void missingMarkerGetsGraceThenRollbackWithoutWritingRedis() {
        MessageExt msg = payload(message());
        msg.setBornTimestamp(System.currentTimeMillis());
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(msg));
        msg.setBornTimestamp(System.currentTimeMillis() - 120000);
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(msg));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertFalse(reservations.hasSuccessMarker(orderId));
    }

    @Test void invalidOrFutureMessageTimestampCannotTriggerEarlyRollback() {
        MessageExt msg = payload(message());
        msg.setBornTimestamp(0);
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(msg));
        msg.setBornTimestamp(System.currentTimeMillis() + 120000);
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(msg));
    }

    @Test void reservationIsIdempotentAndWritesExpiringSuccessMarker() {
        SeckillTxContext ctx = new SeckillTxContext(message());
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).executeLocalTransaction(payload(message()), ctx));
        assertEquals(0, ctx.getLuaResult());
        assertEquals("1", redis.opsForValue().get(SECKILL_TXN_KEY + orderId));
        long ttl = redis.getExpire(SECKILL_TXN_KEY + orderId);
        assertTrue(ttl > 0 && ttl <= 86400);
        assertEquals(0, reservations.reserve(message()));
        assertTrue(redis.getExpire(SECKILL_TXN_KEY + orderId) <= ttl);
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(orderId, reservations.findOriginalOrderId(voucherId, userId));
    }

    @Test void noStockRollsBackWithoutCreatingMarker() {
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "0");
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(message())));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertNull(reservations.findOriginalOrderId(voucherId, userId));
    }

    @Test void duplicateRequestCannotChangeOriginalReservation() {
        reserve(message());
        VoucherOrder other = new VoucherOrder().setId(orderId + 1).setUserId(userId).setVoucherId(voucherId);
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE,
                listener(reservations).executeLocalTransaction(payload(other), new SeckillTxContext(other)));
        assertFalse(reservations.hasSuccessMarker(other.getId()));
        assertEquals(orderId, reservations.findOriginalOrderId(voucherId, userId));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void lostLuaResponseIsRecoveredByFreshProducerCallbackInstance() {
        SeckillReservationService lostResponse = spy(reservations);
        doAnswer(call -> {
            call.callRealMethod();
            throw new SeckillUnavailableException("response lost after script execution");
        }).when(lostResponse).reserve(any());
        SeckillTxContext ctx = new SeckillTxContext(message());
        assertEquals(LocalTransactionState.UNKNOW, listener(lostResponse).executeLocalTransaction(payload(message()), ctx));
        assertEquals(-1, ctx.getLuaResult());
        assertTrue(reservations.hasSuccessMarker(orderId));
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals(0, reservations.reserve(message()));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void expiredMarkerRollsBackCheckButNeverReDeductsOwnedOrder() throws Exception {
        reserve(message());
        redis.expire(SECKILL_TXN_KEY + orderId, 1, TimeUnit.MILLISECONDS);
        Thread.sleep(20);
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals(-1, reservations.reserve(message()));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        service.createOrderFromMQ(message()); // A previously committed CREATE still consumes after expiry.
        service.createOrderFromMQ(message());
        assertEquals(1, count()); assertEquals(9, stock());
    }

    @Test void redisFailureDuringCheckOrCallbackReturnsUnknown() {
        SeckillReservationService unavailable = mock(SeckillReservationService.class);
        when(unavailable.hasSuccessMarker(orderId)).thenThrow(new SeckillUnavailableException("Redis unavailable"));
        when(unavailable.reserve(any())).thenThrow(new SeckillUnavailableException("Redis unavailable"));
        assertEquals(LocalTransactionState.UNKNOW, listener(unavailable).checkLocalTransaction(payload(message())));
        assertEquals(LocalTransactionState.UNKNOW,
                listener(unavailable).executeLocalTransaction(payload(message()), new SeckillTxContext(message())));
        assertEquals(0, count());
    }

    @Test void luaRuntimeErrorDoesNotUndoWritesAndNeedsManualReconciliation() {
        DefaultRedisScript<Long> failAfterStock = new DefaultRedisScript<>(
                "redis.call('DECR',KEYS[1]); return redis.error_reply('injected runtime failure')", Long.class);
        assertThrows(RuntimeException.class, () -> redis.execute(failAfterStock,
                Collections.singletonList(SECKILL_STOCK_KEY + voucherId)));
        assertFalse(reservations.hasSuccessMarker(orderId));
        // Accepted simplification boundary: absence is not proof that no mutation happened.
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(0, count());
    }

    @Test void partialOwnershipWithoutMarkerIsNotReReserved() {
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "9");
        redis.opsForSet().add(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId));
        redis.opsForHash().put(SECKILL_CLAIM_OWNER_KEY + voucherId, String.valueOf(userId), String.valueOf(orderId));
        assertEquals(-1, reservations.reserve(message()));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void nonCanonicalStockIsRejectedBeforeAnyReservationWrite() {
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "01");
        assertEquals(-1, reservations.reserve(message()));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("01", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertNull(reservations.findOriginalOrderId(voucherId, userId));
    }

    @Test void wrongKeyTypeIsRejectedBeforeInventoryMutation() {
        redis.opsForValue().set(SECKILL_CLAIM_OWNER_KEY + voucherId, "invalid");
        assertEquals(-1, reservations.reserve(message()));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(SECKILL_ORDER_KEY + voucherId)));
    }

    @Test void oldHashMarkerIsNotOverwrittenOrSilentlyCommitted() {
        redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "state", "RESERVED");
        assertEquals(-1, reservations.reserve(message()));
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals("RESERVED", redis.opsForHash().get(SECKILL_TXN_KEY + orderId, "state"));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void conflictingOwnershipIsRejectedBeforeMutation() {
        redis.opsForSet().add(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId));
        assertEquals(-1, reservations.reserve(message()));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void callbackContextIdentityMismatchNeverExecutesReservation() {
        VoucherOrder different = new VoucherOrder().setId(orderId + 1).setUserId(userId).setVoucherId(voucherId);
        assertEquals(LocalTransactionState.UNKNOW,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(different)));
        assertFalse(reservations.hasSuccessMarker(orderId));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void concurrentCallbacksAndChecksDeductOnlyOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> jobs = new ArrayList<>();
        try {
            for (int i = 0; i < 12; i++) jobs.add(pool.submit(() -> {
                start.await(); assertEquals(0, reservations.reserve(message()));
                assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
                return null;
            }));
            start.countDown();
            for (Future<?> job : jobs) job.get(10, TimeUnit.SECONDS);
            assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertEquals(1L, redis.opsForSet().size(SECKILL_ORDER_KEY + voucherId));
        } finally { pool.shutdownNow(); }
    }

    @Test void graceDoesNotFenceCallbackResumingAfterRollback() {
        // Explicitly documents the accepted limitation, not a claim of global atomicity.
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals(LocalTransactionState.COMMIT_MESSAGE,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(message())));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(0, count()); // A real Broker may have discarded the half message; audit/manual action is required.
    }

    VoucherOrder extraOrder() {
        VoucherOrder order = new VoucherOrder().setId(IDS.incrementAndGet()).setUserId(IDS.incrementAndGet()).setVoucherId(voucherId);
        extraKeys.add(SECKILL_TXN_KEY + order.getId());
        return order;
    }

    @Test void concurrentUsersCannotOversellAndConsumerReplaysDoNotDeductTwice() throws Exception {
        List<VoucherOrder> candidates = new ArrayList<>();
        for (int i = 0; i < 24; i++) candidates.add(extraOrder());
        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        try {
            for (VoucherOrder order : candidates) results.add(pool.submit(() -> {
                start.await();
                if (reservations.reserve(order) != 0) return false;
                service.createOrderFromMQ(order);
                service.createOrderFromMQ(order);
                return true;
            }));
            start.countDown();
            int accepted = 0;
            for (Future<Boolean> future : results) if (future.get(20, TimeUnit.SECONDS)) accepted++;
            assertEquals(10, accepted); assertEquals(10, count()); assertEquals(0, stock());
            assertEquals("0", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        } finally { pool.shutdownNow(); }
    }
}
