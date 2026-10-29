package com.hmdp.order;

import com.hmdp.config.WebExceptionAdvice;
import com.hmdp.controller.VoucherOrderController;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.utils.*;
import com.hmdp.service.SeckillReservationService;
import com.hmdp.service.SeckillUnavailableException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Arrays;
import java.util.Collections;

import static com.hmdp.utils.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Real limiter, Lua, order service and MySQL; MQ transport is mocked as in the other order tests. */
class OrderSubmissionIntegrationTest extends OrderIntegrationSupport {
    @MockBean RedisIdWorker ids;
    MockMvc mvc;
    SlidingWindowRateLimiter limiter;
    String token;

    @BeforeEach void setupWeb() {
        token = "submission-test-" + userId;
        redis.opsForHash().putAll(LOGIN_USER_KEY + token, Collections.singletonMap("id", String.valueOf(userId)));
        when(ids.nextId("order")).thenReturn(orderId);
        limiter = new SlidingWindowRateLimiter();
        ReflectionTestUtils.setField(limiter, "stringRedisTemplate", redis);
        SlidingWindowInterceptor interceptor = new SlidingWindowInterceptor();
        ReflectionTestUtils.setField(interceptor, "slidingWindowRateLimiter", limiter);
        ReflectionTestUtils.setField(interceptor, "enabled", true);
        ReflectionTestUtils.setField(interceptor, "windowMs", 30000L);
        ReflectionTestUtils.setField(interceptor, "maxRequests", 1);
        interceptor.validateConfiguration();
        VoucherOrderController controller = new VoucherOrderController();
        ReflectionTestUtils.setField(controller, "voucherOrderService", service);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new WebExceptionAdvice())
                .addInterceptors(new RefreshTokenInterceptor(redis), new LoginInterceptor())
                .addMappedInterceptors(new String[]{SlidingWindowInterceptor.SUBMISSION_PATH,
                        SlidingWindowInterceptor.SUBMISSION_PATH + "/"}, interceptor)
                .build();
    }

    @AfterEach void cleanupWeb() {
        UserHolder.removeUser();
        ReflectionTestUtils.setField(service, "reservations", reservations);
        redis.delete(Arrays.asList(LOGIN_USER_KEY + token, RateLimitConstants.SLIDING_WINDOW_SECKILL_KEY + userId));
    }

    @Test
    void acceptedOrderCompletesButOverQuotaSubmissionHasNoSideEffects() throws Exception {
        SendResult sent = new SendResult();
        sent.setSendStatus(SendStatus.SEND_OK);
        when(producer.sendOrderCreateInTransaction(any(), any())).thenAnswer(call -> {
            SeckillTxContext context = call.getArgument(1);
            context.setLuaResult(reservations.reserve(message()));
            return sent;
        });

        mvc.perform(post("/voucher-order/seckill/" + voucherId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").value(orderId));
        assertEquals(0, count());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));

        mvc.perform(post("/voucher-order/seckill/" + voucherId).header("authorization", token))
                .andExpect(status().isTooManyRequests());
        verify(ids, times(1)).nextId("order");
        verify(producer, times(1)).sendOrderCreateInTransaction(any(), any());

        VoucherOrder order = message();
        service.createOrderFromMQ(order);
        assertEquals(1, count());
        assertEquals(9, stock());
        assertEquals("9", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.status").value("SUCCESS"));
        assertEquals(1L, redis.opsForZSet().zCard(RateLimitConstants.SLIDING_WINDOW_SECKILL_KEY + userId));
    }

    @Test
    void unavailableLimiterStopsBeforeIdStockQueueOrMqChanges() throws Exception {
        StringRedisTemplate unavailable = mock(StringRedisTemplate.class);
        when(unavailable.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("limiter unavailable"));
        ReflectionTestUtils.setField(limiter, "stringRedisTemplate", unavailable);
        mvc.perform(post("/voucher-order/seckill/" + voucherId).header("authorization", token))
                .andExpect(status().isServiceUnavailable());
        verifyNoInteractions(ids, producer);
        assertEquals(0, count());
        assertEquals(10, stock());
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals("PREPARED", reservations.read(orderId).get("state"));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(SECKILL_CLAIM_OWNER_KEY + voucherId)));
        assertEquals("PREPARED", reservations.read(orderId).get("state"));
    }

    @Test void lostSubmissionResponseReturnsOriginalIdAndVoucherQueryFindsIt() throws Exception {
        when(producer.sendOrderCreateInTransaction(any(), any())).thenAnswer(call -> {
            reservations.reserve(message());
            throw new IllegalStateException("response lost after local transaction");
        });
        mvc.perform(post("/voucher-order/seckill/" + voucherId).header("authorization", token))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.data.orderId").value(orderId))
                .andExpect(jsonPath("$.data.status").value("UNKNOWN"));
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/" + voucherId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.orderId").value(orderId))
                .andExpect(jsonPath("$.data.status").value("PROCESSING"));
        service.createOrderFromMQ(message());
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/" + voucherId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.status").value("SUCCESS"));
    }

    @Test void sendFailureLeavesPreparationThatCanBeTerminatedWithoutReservation() throws Exception {
        when(producer.sendOrderCreateInTransaction(any(), any())).thenThrow(new IllegalStateException("send failed"));
        mvc.perform(post("/voucher-order/seckill/" + voucherId).header("authorization", token))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.data.orderId").value(orderId));
        redis.opsForHash().put(SECKILL_TXN_KEY + orderId, "deadline", "1");
        recovery.discover();
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.status").value("REJECTED"))
                .andExpect(jsonPath("$.data.transactionState").value("ABORTED"));
        assertEquals("10", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test void redisQueryFailureIs503RatherThanNotFound() throws Exception {
        SeckillReservationService unavailable = mock(SeckillReservationService.class);
        when(unavailable.findOriginalOrderId(voucherId, userId)).thenThrow(new SeckillUnavailableException("Redis unavailable"));
        when(unavailable.read(orderId)).thenThrow(new SeckillUnavailableException("Redis unavailable"));
        ReflectionTestUtils.setField(service, "reservations", unavailable);
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/" + voucherId).header("authorization", token))
                .andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.success").value(false));
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(status().isServiceUnavailable());
    }

    @Test void committedOrderIsQueryableEvenIfRedisTransactionRecordIsLost() throws Exception {
        service.createOrderFromMQ(reservedMessage());
        redis.delete(SECKILL_TXN_KEY + orderId);
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.status").value("SUCCESS"));
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/" + voucherId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.orderId").value(orderId));
    }

    @Test void anotherUserCannotReadReservationOrPayOrder() throws Exception {
        reserve(message());
        redis.opsForHash().put(LOGIN_USER_KEY + token, "id", String.valueOf(userId + 1));
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(jsonPath("$.success").value(false)).andExpect(jsonPath("$.data").doesNotExist());
        service.createOrderFromMQ(message());
        mvc.perform(get("/voucher-order/seckill/result/" + orderId).header("authorization", token))
                .andExpect(jsonPath("$.success").value(false));
        mvc.perform(put("/voucher-order/pay/" + orderId).header("authorization", token))
                .andExpect(jsonPath("$.success").value(false));
        assertEquals(ORDER_STATUS_UNPAID, orders.selectById(orderId).getStatus());
    }

    @Test void originalOwnershipWithMissingRecordReturnsUnknownRatherThanNotFound() throws Exception {
        reserve(message()); redis.delete(SECKILL_TXN_KEY + orderId);
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/" + voucherId).header("authorization", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data.status").value("UNKNOWN"))
                .andExpect(jsonPath("$.data.orderId").value(orderId));
    }
}
