package com.hmdp.order;

import com.hmdp.utils.OrderTimeoutPolicy;
import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.*;

class OrderTimeoutPolicyTest {
    private final LocalDateTime created = LocalDateTime.of(2026, 1, 1, 10, 0);

    @Test void newOrderUsesFifteenMinutes() {
        assertEquals(15, OrderTimeoutPolicy.delayLevel(created, created));
    }
    @Test void retryUsesOriginalDeadline() {
        assertEquals(9, OrderTimeoutPolicy.delayLevel(created, created.plusMinutes(10)));
        assertEquals(4, OrderTimeoutPolicy.delayLevel(created, created.plusMinutes(14).plusSeconds(20)));
    }
    @Test void expiredOrderIsSentImmediately() {
        assertEquals(0, OrderTimeoutPolicy.delayLevel(created, created.plusMinutes(15)));
        assertEquals(0, OrderTimeoutPolicy.delayLevel(created, created.plusMinutes(16)));
    }
    @Test void SubsecondRemainderUsesMinimumDelay() {
        assertEquals(1, OrderTimeoutPolicy.delayLevel(created, created.plusMinutes(15).minusNanos(500_000_000)));
    }
    @Test void missingTimestampCannotRestartDeadline() {
        assertThrows(NullPointerException.class, () -> OrderTimeoutPolicy.delayLevel(null, created));
    }
}
