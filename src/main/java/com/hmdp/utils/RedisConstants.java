package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final Long CACHE_VOUCHER_TTL = 30L;
    public static final String CACHE_VOUCHER_KEY = "cache:voucher:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    /** Redisson hash locks must not share keys with the legacy SETNX string locks. */
    public static final String CACHE_REBUILD_LOCK_KEY = "lock:cache-rebuild:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /** 秒杀成功 claim 用户集合（Lua 脚本 SADD 写入，一人一单与缺单核查依据） */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    /** Hash: userId -> original orderId. No short TTL: must survive MQ retries / manual replay. */
    public static final String SECKILL_CLAIM_OWNER_KEY = "seckill:claim-owner:";
    /** Permanent idempotency marker for cancelled order stock refunds; no automatic cleanup. */
    public static final String SECKILL_STOCK_RETURNED_KEY = "seckill:stock-returned:";
    /** RocketMQ 事务成功 String 标记：值为 1，与 Lua 预扣同脚本写入并设置 TTL */
    public static final String SECKILL_TXN_KEY = "seckill:txn:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    /** 订单状态：1未支付 2已支付 3已核销 4已取消 5退款中 6已退款 */
    public static final int ORDER_STATUS_UNPAID = 1;
    public static final int ORDER_STATUS_PAID = 2;
    public static final int ORDER_STATUS_CANCELLED = 4;
}
