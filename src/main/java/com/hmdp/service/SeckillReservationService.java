package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import static com.hmdp.utils.RedisConstants.*;

/** Atomic reservation and expiring success marker; durable ownership is independent of that marker. */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeckillReservationService {
    public static final long SUCCESS = 0, NO_STOCK = 1, DUPLICATE = 2, UNKNOWN = -1;
    private final StringRedisTemplate redis;
    @Value("${seckill.transaction.marker-ttl-seconds:86400}") private long markerTtlSeconds;
    @Value("${seckill.transaction.check-grace-ms:60000}") private long checkGraceMs;
    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>();
    static {
        SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SCRIPT.setResultType(Long.class);
    }

    @PostConstruct public void validateConfiguration() {
        if (markerTtlSeconds <= 0 || markerTtlSeconds > Integer.MAX_VALUE
                || checkGraceMs < 0 || checkGraceMs >= markerTtlSeconds * 1000) {
            throw new IllegalArgumentException("Require 0 <= check-grace-ms < marker-ttl-seconds * 1000 (TTL <= Integer.MAX_VALUE)");
        }
    }

    /** Called only by the producer local transaction, AFTER the half message was sent. */
    public long reserve(VoucherOrder order) {
        validateIdentity(order);
        try {
            Long result = redis.execute(SCRIPT, Arrays.asList(SECKILL_TXN_KEY + order.getId(),
                            SECKILL_STOCK_KEY + order.getVoucherId(), SECKILL_ORDER_KEY + order.getVoucherId(),
                            SECKILL_CLAIM_OWNER_KEY + order.getVoucherId()),
                    order.getVoucherId().toString(), order.getUserId().toString(), order.getId().toString(),
                    String.valueOf(markerTtlSeconds));
            if (result == null || result < -1 || result > 2) {
                throw new SeckillUnavailableException("Unexpected reservation result: " + result);
            }
            if (result == UNKNOWN) log.error("SECKILL_RESERVATION_UNCERTAIN: orderId={}; inspect stock/ownership manually", order.getId());
            return result;
        } catch (DataAccessException e) {
            throw new SeckillUnavailableException("Reservation Redis operation failed: " + order.getId(), e);
        }
    }

    /** Missing means no available success evidence, NOT proof that no stock was deducted. */
    public boolean hasSuccessMarker(Long orderId) {
        try {
            String marker = redis.opsForValue().get(SECKILL_TXN_KEY + orderId);
            if (marker == null) return false;
            if (!"1".equals(marker)) throw new SeckillUnavailableException("Invalid success marker: " + orderId);
            return true;
        } catch (DataAccessException e) { throw new SeckillUnavailableException("Cannot read success marker", e); }
    }

    public Long findOriginalOrderId(Long voucherId, Long userId) {
        try {
            Object owner = redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, userId.toString());
            boolean member = Boolean.TRUE.equals(redis.opsForSet().isMember(SECKILL_ORDER_KEY + voucherId, userId.toString()));
            if ((owner != null) != member) throw new SeckillUnavailableException("Conflicting purchaser evidence");
            if (owner == null) return null;
            if (!Long.valueOf(-1).equals(redis.getExpire(SECKILL_CLAIM_OWNER_KEY + voucherId))
                    || !Long.valueOf(-1).equals(redis.getExpire(SECKILL_ORDER_KEY + voucherId))) {
                throw new SeckillUnavailableException("Expiring purchaser evidence");
            }
            Long id = Long.valueOf(owner.toString());
            if (id <= 0) throw new NumberFormatException("nonpositive owner");
            return id;
        } catch (DataAccessException | NumberFormatException e) {
            throw new SeckillUnavailableException("Cannot find original order", e);
        }
    }

    public static void validateIdentity(VoucherOrder order) {
        if (order == null || order.getId() == null || order.getId() <= 0
                || order.getUserId() == null || order.getUserId() <= 0
                || order.getVoucherId() == null || order.getVoucherId() <= 0) {
            throw new IllegalArgumentException("Incomplete order identity");
        }
    }
}
