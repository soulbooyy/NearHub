package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;

/**
 * 秒杀提交按 userId 限流（需在登录拦截器之后），不提供全局流量保护。
 */
@Slf4j
@Component
public class SlidingWindowInterceptor implements HandlerInterceptor {

    // MvcConfig 只注册此单层路径；HTTP 方法必须在 preHandle 中单独判断。
    public static final String SUBMISSION_PATH = "/voucher-order/seckill/*";

    @Resource
    private SlidingWindowRateLimiter slidingWindowRateLimiter;

    @Value("${seckill.rate-limit.sliding-window.enabled:true}")
    private boolean enabled;

    @Value("${seckill.rate-limit.sliding-window.window-ms:1000}")
    private long windowMs;

    @Value("${seckill.rate-limit.sliding-window.max-requests:5}")
    private int maxRequests;

    @PostConstruct
    public void validateConfiguration() {
        if (enabled && (windowMs <= 0 || maxRequests <= 0)) {
            throw new IllegalArgumentException("Sliding window duration and max requests must be positive");
        }
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        if (!enabled || !HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }
        if (UserHolder.getUser() == null) {
            return true; // 交给登录拦截器处理
        }
        Long userId = UserHolder.getUser().getId();
        String key = RateLimitConstants.SLIDING_WINDOW_SECKILL_KEY + userId;
        if (slidingWindowRateLimiter.tryAcquire(key, windowMs, maxRequests)) {
            return true;
        }
        log.warn("滑动窗口限流触发, userId={}, uri={}", userId, request.getRequestURI());
        response.setStatus(429);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write(JSONUtil.toJsonStr(Result.fail("请求过于频繁，请稍后再试")));
        return false;
    }
}
