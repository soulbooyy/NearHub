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

/** Real Redis/MySQL; directly invokes real callbacks without a running Broker. */
class SeckillTransactionIntegrationTest extends OrderIntegrationSupport {
    final List<String> extraKeys = new ArrayList<>();
    @AfterEach void cleanExtraKeys() { if (!extraKeys.isEmpty()) redis.delete(extraKeys); }

    SeckillTransactionListener listener(SeckillReservationService storage) {
        SeckillTransactionListener listener = new SeckillTransactionListener();
        ReflectionTestUtils.setField(listener, "reservations", storage);
        ReflectionTestUtils.setField(listener, "orders", orders);
        return listener;
    }
    MessageExt payload(VoucherOrder order) {
        MessageExt message = new MessageExt();
        message.setBody(JSONUtil.toJsonStr(order).getBytes(StandardCharsets.UTF_8));
        return message;
    }
    void expirePreparation() { redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "deadline", "1"); }
    String state() { return (String) reservations.read(orderId).get("state"); }

    @Test void preparedCheckDoesNotReserveStock() {
        assertEquals("PREPARED", state());
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void reservationIsIdempotentAndEvidenceHasNoAutomaticExpiry() {
        SeckillTxContext ctx = new SeckillTxContext(message());
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).executeLocalTransaction(payload(message()), ctx));
        assertEquals(0, ctx.getLuaResult());
        assertEquals(0, reservations.reserve(message()));
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(-1L, redis.getExpire(SECKILL_TXN_KEY + orderId));
    }

    @Test void businessRejectionIsARecordedRollbackDecision() {
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "0");
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(message())));
        assertEquals("REJECTED", state());
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertFalse(redis.hasKey(SECKILL_CLAIM_OWNER_KEY + voucherId));
    }

    @Test void duplicateRequestCannotChangeOriginalReservation() {
        reserve(message());
        VoucherOrder other = new VoucherOrder().setId(orderId + 1).setUserId(userId).setVoucherId(voucherId);
        reservations.prepare(other);
        assertEquals(2, reservations.reserve(other));
        assertEquals(String.valueOf(orderId), reservations.read(other.getId()).get("originalOrderId"));
        assertEquals("RESERVED", state());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void preparedExpiryPreventsLateCallbackAndDoesNotTouchStock() {
        expirePreparation();
        recovery.discover();
        assertEquals("ABORTED", state());
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(message())));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
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
        assertEquals("RESERVED", state());
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals(0, reservations.reserve(message()));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void missingOrExpiringRecordNeverMeansRollbackOrNewReservation() {
        redis.expire(SECKILL_TXN_KEY + orderId, 10, TimeUnit.SECONDS);
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(payload(message())));
        redis.delete(SECKILL_TXN_KEY + orderId);
        assertEquals(LocalTransactionState.UNKNOW, listener(reservations).checkLocalTransaction(payload(message())));
        assertEquals(-1, reservations.reserve(message()));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void committedOrderCanResolveCheckWhenRedisEvidenceIsUnavailable() {
        service.createOrderFromMQ(reservedMessage());
        redis.delete(SECKILL_TXN_KEY + orderId);
        SeckillReservationService unavailable = mock(SeckillReservationService.class);
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener(unavailable).checkLocalTransaction(payload(message())));
        verifyNoInteractions(unavailable);
    }

    @Test void redisFailureDuringCheckReturnsUnknown() {
        SeckillReservationService unavailable = mock(SeckillReservationService.class);
        when(unavailable.check(any())).thenThrow(new SeckillUnavailableException("Redis unavailable"));
        assertEquals(LocalTransactionState.UNKNOW, listener(unavailable).checkLocalTransaction(payload(message())));
        assertEquals(0, count());
    }

    @Test void partialWriteDiagnosticBlocksAbortAndRepeatDeduction() {
        DefaultRedisScript<Long> failAfterStock = new DefaultRedisScript<>(
                "redis.call('HSET',KEYS[1],'writePending','1'); redis.call('DECR',KEYS[2]); "
                        + "return redis.error_reply('injected runtime failure')", Long.class);
        assertThrows(RuntimeException.class, () -> redis.execute(failAfterStock,
                Arrays.asList(SECKILL_TXN_KEY + orderId, SECKILL_STOCK_KEY + voucherId)));
        expirePreparation();
        assertEquals(-1, reservations.check(message()));
        assertEquals(-1, reservations.reserve(message()));
        recovery.discover();
        assertEquals("PREPARED", state());
        assertEquals("1", reservations.read(orderId).get("writePending"));
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(0, count());
    }

    @Test void nonCanonicalStockIsRejectedBeforeAnyReservationWrite() {
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "01");
        assertEquals(-1, reservations.reserve(message()));
        assertEquals("0", reservations.read(orderId).get("writePending"));
        assertEquals("1", reservations.read(orderId).get("review"));
        assertEquals("01", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertNull(reservations.findOriginalOrderId(voucherId, userId));
    }

    @Test void wrongKeyTypeIsRejectedBeforeInventoryMutation() {
        redis.opsForValue().set(SECKILL_CLAIM_OWNER_KEY + voucherId, "invalid");
        assertEquals(-1, reservations.reserve(message()));
        assertEquals("0", reservations.read(orderId).get("writePending"));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(SECKILL_ORDER_KEY + voucherId)));
    }

    @Test void conflictingOwnershipCannotBeCommittedOrAborted() {
        redis.opsForSet().add(SECKILL_ORDER_KEY + voucherId, String.valueOf(userId));
        expirePreparation();
        assertEquals(-1, reservations.check(message()));
        assertEquals("1", reservations.read(orderId).get("review"));
        assertEquals("PREPARED", state());
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void callbackContextIdentityMismatchNeverExecutesReservation() {
        VoucherOrder different = new VoucherOrder().setId(orderId + 1).setUserId(userId).setVoucherId(voucherId);
        assertEquals(LocalTransactionState.UNKNOW,
                listener(reservations).executeLocalTransaction(payload(message()), new SeckillTxContext(different)));
        assertEquals("PREPARED", state());
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void expiryAndReservationRaceCannotLeaveAbortedDeduction() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 12; i++) {
                VoucherOrder order = extraOrder();
                if (i % 2 == 0) redis.opsForHash().put(SECKILL_TXN_KEY + order.getId(), "deadline", "1");
                long before = Long.parseLong(redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
                CountDownLatch start = new CountDownLatch(1);
                Future<Long> callback = pool.submit(() -> { start.await(); return reservations.reserve(order); });
                Future<Long> check = pool.submit(() -> { start.await(); return reservations.check(order); });
                start.countDown(); callback.get(5, TimeUnit.SECONDS); check.get(5, TimeUnit.SECONDS);
                String state = (String) reservations.read(order.getId()).get("state");
                long after = Long.parseLong(redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
                assertEquals("RESERVED".equals(state) ? before - 1 : before, after);
                assertTrue(Arrays.asList("RESERVED", "ABORTED").contains(state));
            }
        } finally { pool.shutdownNow(); }
    }

    VoucherOrder extraOrder() {
        VoucherOrder order = new VoucherOrder().setId(IDS.incrementAndGet()).setUserId(IDS.incrementAndGet()).setVoucherId(voucherId);
        extraKeys.add(SECKILL_TXN_KEY + order.getId());
        reservations.prepare(order);
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
