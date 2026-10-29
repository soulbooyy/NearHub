package com.hmdp.config;

import com.hmdp.dto.Result;
import com.hmdp.service.CacheRebuildBusyException;
import com.hmdp.service.SeckillUnavailableException;
import com.hmdp.service.SeckillPendingException;
import com.hmdp.utils.RateLimitUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

    @ExceptionHandler(SeckillPendingException.class)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Result handleSeckillPending(SeckillPendingException e) {
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        data.put("orderId", e.getOrderId());
        data.put("status", "UNKNOWN");
        return Result.ok(data);
    }

    @ExceptionHandler(SeckillUnavailableException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Result handleSeckillUnavailable(SeckillUnavailableException e) {
        log.error("SECKILL_UNAVAILABLE", e);
        return Result.fail("秒杀状态暂不可用，请稍后重试");
    }

    @ExceptionHandler(RateLimitUnavailableException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Result handleRateLimitUnavailable(RateLimitUnavailableException e) {
        log.error("SECKILL_RATE_LIMIT_UNAVAILABLE", e);
        return Result.fail("秒杀限流服务暂不可用，请稍后重试");
    }

    @ExceptionHandler(CacheRebuildBusyException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Result handleCacheRebuildBusy(CacheRebuildBusyException e) {
        log.warn(e.getMessage());
        return Result.fail("商铺信息正在加载，请稍后重试");
    }

    @ExceptionHandler(RuntimeException.class)
    public Result handleRuntimeException(RuntimeException e) {
        log.error(e.toString(), e);
        return Result.fail("服务器异常");
    }
}
