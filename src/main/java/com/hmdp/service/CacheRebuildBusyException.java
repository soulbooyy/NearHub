package com.hmdp.service;

/** Temporary contention/interruption, never a "shop not found" result. */
public class CacheRebuildBusyException extends RuntimeException {
    public CacheRebuildBusyException(String message) { super(message); }
    public CacheRebuildBusyException(String message, Throwable cause) { super(message, cause); }
}
