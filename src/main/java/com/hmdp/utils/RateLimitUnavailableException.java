package com.hmdp.utils;

/** 限流组件无法判定额度时拒绝提交，与用户超限及其他业务故障分开处理。 */
public class RateLimitUnavailableException extends RuntimeException {

    public RateLimitUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RateLimitUnavailableException(String message) {
        super(message);
    }
}
