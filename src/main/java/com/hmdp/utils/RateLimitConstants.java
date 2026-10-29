package com.hmdp.utils;

/**
 * 秒杀提交的用户维度滑动窗口 Redis Key
 */
public class RateLimitConstants {

    private RateLimitConstants() {
    }

    /** 业务滑动窗口：rate:sw:seckill:{userId} */
    public static final String SLIDING_WINDOW_SECKILL_KEY = "rate:sw:seckill:";
}
