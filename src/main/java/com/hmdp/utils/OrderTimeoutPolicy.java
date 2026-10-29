package com.hmdp.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/** Must match docker/rocketmq/broker.conf. Early deliveries are checked against DB create_time. */
public final class OrderTimeoutPolicy {
    public static final int TIMEOUT_MINUTES = 15;
    private static final int[] DELAY_SECONDS = {1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 900};

    private OrderTimeoutPolicy() { }

    public static LocalDateTime deadline(LocalDateTime createdAt) {
        return Objects.requireNonNull(createdAt, "Order create_time is required").plusMinutes(TIMEOUT_MINUTES);
    }

    public static int delayLevel(LocalDateTime createdAt, LocalDateTime now) {
        long remainingMs = Duration.between(now, deadline(createdAt)).toMillis();
        if (remainingMs <= 0) {
            return 0; // Expired: send immediately, do not restart a 15-minute timer.
        }
        for (int i = DELAY_SECONDS.length - 1; i >= 0; i--) {
            if (DELAY_SECONDS[i] * 1000L <= remainingMs) {
                return i + 1;
            }
        }
        return 1;
    }
}
