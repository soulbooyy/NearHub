package com.hmdp.ratelimit;

import com.hmdp.config.MvcConfig;
import com.hmdp.config.WebExceptionAdvice;
import com.hmdp.controller.VoucherOrderController;
import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Uses the real MVC registration, authentication interceptors and rate limiter. */
@WebMvcTest
@ContextConfiguration(classes = {VoucherOrderController.class, SlidingWindowWebTest.ProbeController.class,
        MvcConfig.class, SlidingWindowInterceptor.class, SlidingWindowRateLimiter.class, WebExceptionAdvice.class})
class SlidingWindowWebTest {
    @Autowired MockMvc mvc;
    @MockBean StringRedisTemplate redis;
    @MockBean IVoucherOrderService orders;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        UserHolder.removeUser();
        HashOperations<String, String, String> sessions = mock(HashOperations.class);
        when(redis.<String, String>opsForHash()).thenReturn(sessions);
        when(sessions.entries(RedisConstants.LOGIN_USER_KEY + "valid"))
                .thenReturn(Collections.singletonMap("id", "42"));
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString())).thenReturn(1L);
    }

    @Test void acceptedSubmissionReachesBusinessAndCleansUserContext() throws Exception {
        when(orders.seckillVoucher(7L)).thenReturn(Result.ok(99L));
        mvc.perform(post("/voucher-order/seckill/7").header("authorization", "valid"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data").value(99L));
        verify(redis).execute(any(RedisScript.class), eq(Collections.singletonList("rate:sw:seckill:42")),
                anyString(), anyString(), anyString(), anyString());
        verify(orders).seckillVoucher(7L);
        assertNull(UserHolder.getUser());
    }

    @ParameterizedTest @ValueSource(strings = {"/voucher-order/seckill/7", "/voucher-order/seckill/7/"})
    void exhaustedWindowReturns429WithoutCallingBusiness(String path) throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString())).thenReturn(0L);
        mvc.perform(post(path).header("authorization", "valid"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("请求过于频繁，请稍后再试"));
        verifyNoInteractions(orders);
        assertNull(UserHolder.getUser());
    }

    @Test void limiterRedisFailureReturns503WithoutCallingBusiness() throws Exception {
        RedisConnectionFailureException failure = new RedisConnectionFailureException("unavailable");
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString())).thenThrow(failure);
        mvc.perform(post("/voucher-order/seckill/7").header("authorization", "valid"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("秒杀限流服务暂不可用，请稍后重试"))
                .andExpect(result -> {
                    assertTrue(result.getResolvedException() instanceof RateLimitUnavailableException);
                    assertSame(failure, result.getResolvedException().getCause());
                });
        verifyNoInteractions(orders);
        assertNull(UserHolder.getUser());
    }

    @Test void missingScriptResultIsUnavailableRatherThanUserOverQuota() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString())).thenReturn(null);
        mvc.perform(post("/voucher-order/seckill/7").header("authorization", "valid"))
                .andExpect(status().isServiceUnavailable());
        verifyNoInteractions(orders);
    }

    @Test void unauthenticatedSubmissionStopsBeforeRateLimit() throws Exception {
        mvc.perform(post("/voucher-order/seckill/7")).andExpect(status().isUnauthorized());
        verifyNoInteractions(redis, orders);
    }

    @Test void pollingDoesNotConsumeSubmissionQuota() throws Exception {
        when(orders.getSeckillResult(99L)).thenReturn(Result.ok("WAITING"));
        mvc.perform(get("/voucher-order/seckill/result/99").header("authorization", "valid"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.data").value("WAITING"));
        verify(orders).getSeckillResult(99L);
        verifyNoScript();
    }

    @Test void voucherLookupDoesNotConsumeSubmissionQuota() throws Exception {
        when(orders.getSeckillResultByVoucher(7L)).thenReturn(Result.ok("PROCESSING"));
        mvc.perform(get("/voucher-order/seckill/result/by-voucher/7").header("authorization", "valid"))
                .andExpect(status().isOk());
        verify(orders).getSeckillResultByVoucher(7L);
        verifyNoScript();
    }

    @Test void nonPostOnMatchingSingleSegmentPathDoesNotConsumeQuota() throws Exception {
        mvc.perform(get("/voucher-order/seckill/probe").header("authorization", "valid"))
                .andExpect(status().isOk());
        verifyNoScript();
    }

    @Test void nestedPostAndUnrelatedPostDoNotConsumeQuota() throws Exception {
        mvc.perform(post("/voucher-order/seckill/result/probe").header("authorization", "valid"))
                .andExpect(status().isOk());
        mvc.perform(post("/rate-limit-probe").header("authorization", "valid"))
                .andExpect(status().isOk());
        verifyNoScript();
    }

    @Test void paymentDoesNotConsumeQuota() throws Exception {
        when(orders.payOrder(99L)).thenReturn(Result.ok());
        mvc.perform(put("/voucher-order/pay/99").header("authorization", "valid"))
                .andExpect(status().isOk());
        verify(orders).payOrder(99L);
        verifyNoScript();
    }

    @Test void businessRedisFailureIsNotRelabeledAsLimiterFailure() throws Exception {
        when(orders.seckillVoucher(7L)).thenThrow(new RedisConnectionFailureException("business failure"));
        mvc.perform(post("/voucher-order/seckill/7").header("authorization", "valid"))
                .andExpect(status().isOk()) // Existing generic exception contract is outside this refactor.
                .andExpect(jsonPath("$.errorMsg").value("服务器异常"));
    }

    private void verifyNoScript() {
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString());
    }

    @RestController
    static class ProbeController {
        @GetMapping("/voucher-order/seckill/probe")
        public Result getOnSubmissionPath() { return Result.ok(); }
        @PostMapping({"/voucher-order/seckill/result/probe", "/rate-limit-probe"})
        public Result postOutsideSubmissionPath() { return Result.ok(); }
    }
}
