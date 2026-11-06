package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import static com.hmdp.utils.RedisConstants.*;

/** Atomic reservation with permanent ownership written last as its success evidence. */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeckillReservationService {
    public static final long SUCCESS = 0, NO_STOCK = 1, DUPLICATE = 2, UNKNOWN = -1;
    private final StringRedisTemplate redis;
    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>();
    static {
        SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SCRIPT.setResultType(Long.class);
    }

    /** Called only by the producer local transaction, AFTER the half message was sent. */
    public long reserve(VoucherOrder order) {
        validateIdentity(order);
        try {
            Long result = redis.execute(SCRIPT, Arrays.asList(SECKILL_STOCK_KEY + order.getVoucherId(),
                            SECKILL_CLAIM_OWNER_KEY + order.getVoucherId()),
                    order.getVoucherId().toString(), order.getUserId().toString(), order.getId().toString());
            if (result == null || result < -1 || result > 2) {
                throw new SeckillUnavailableException("Unexpected reservation result: " + result);
            }
            if (result == UNKNOWN) log.error("SECKILL_RESERVATION_UNCERTAIN: orderId={}; inspect stock/ownership manually", order.getId());
            return result;
        } catch (DataAccessException e) {
            throw new SeckillUnavailableException("Reservation Redis operation failed: " + order.getId(), e);
        }
    }

    /** Missing ownership is not proof that no stock was deducted (runtime errors/data loss). */
    public Long findOriginalOrderId(Long voucherId, Long userId) {
        try {
            Object owner = redis.opsForHash().get(SECKILL_CLAIM_OWNER_KEY + voucherId, userId.toString());
            if (owner == null) return null;
            if (!Long.valueOf(-1).equals(redis.getExpire(SECKILL_CLAIM_OWNER_KEY + voucherId))) {
                throw new SeckillUnavailableException("Expiring ownership evidence");
            }
            String value = owner.toString();
            if (!value.matches("[1-9][0-9]*")) throw new NumberFormatException("noncanonical owner");
            Long id = Long.valueOf(value);
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
