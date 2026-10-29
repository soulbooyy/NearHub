package com.hmdp.service;

/** Failure to read/write the evidence needed to answer a seckill request. */
public class SeckillUnavailableException extends RuntimeException {
    public SeckillUnavailableException(String message, Throwable cause) { super(message, cause); }
    public SeckillUnavailableException(String message) { super(message); }
}
