package com.hmdp.service;

import lombok.Getter;

/** Submission may have executed; return its identity instead of inviting a blind new request. */
@Getter
public class SeckillPendingException extends RuntimeException {
    private final Long orderId;
    public SeckillPendingException(Long orderId, Throwable cause) {
        super("Seckill submission outcome is uncertain", cause);
        this.orderId = orderId;
    }
}
