package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import java.nio.charset.StandardCharsets;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import static com.hmdp.utils.RedisConstants.*;

/** One Lua protocol for preparation, reservation and transaction checks. No automatic evidence expiry. */
@Slf4j
@Service
@RequiredArgsConstructor
public class SeckillReservationService {
    public static final long RESERVED = 0, NO_STOCK = 1, DUPLICATE = 2, ABORTED = 3, UNKNOWN = -1;
    private final StringRedisTemplate redis;
    @Value("${seckill.transaction.prepare-timeout-ms:60000}") private long prepareTimeoutMs;
    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>();
    static {
        SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SCRIPT.setResultType(Long.class);
    }

    @PostConstruct public void validateConfiguration() {
        if (prepareTimeoutMs <= 0) throw new IllegalArgumentException("prepare-timeout-ms must be positive");
    }

    public void prepare(VoucherOrder order) {
        if (execute("PREPARE", order) != 4) {
            throw new SeckillUnavailableException("Cannot prepare order " + order.getId());
        }
    }

    public long reserve(VoucherOrder order) { return execute("RESERVE", order); }
    public long check(VoucherOrder order) { return execute("CHECK", order); }

    private long execute(String action, VoucherOrder order) {
        validateIdentity(order);
        try {
            Long result = redis.execute(SCRIPT, Arrays.asList(SECKILL_TXN_KEY + order.getId(),
                            SECKILL_STOCK_KEY + order.getVoucherId(), SECKILL_ORDER_KEY + order.getVoucherId(),
                            SECKILL_CLAIM_OWNER_KEY + order.getVoucherId()),
                    action, order.getVoucherId().toString(), order.getUserId().toString(), order.getId().toString(),
                    String.valueOf(prepareTimeoutMs));
            if (result == null || result < -1 || result > 4) {
                throw new SeckillUnavailableException("Unexpected reservation result: " + result);
            }
            // UNKNOWN is also the normal PREPARED check result. Avoid error-level logs on every pending poll.
            if (result == UNKNOWN) log.debug("SECKILL_EVIDENCE_UNCERTAIN: action={}, orderId={}", action, order.getId());
            return result;
        } catch (DataAccessException e) {
            throw new SeckillUnavailableException("Reservation Redis operation failed: " + order.getId(), e);
        }
    }

    public Map<Object, Object> read(Long orderId) {
        try {
            Map<Object, Object> record = redis.opsForHash().entries(SECKILL_TXN_KEY + orderId);
            if (!record.isEmpty()) {
                identity(orderId, record);
                if (!Arrays.asList("PREPARED", "RESERVED", "REJECTED", "ABORTED").contains(record.get("state"))
                        || !Long.valueOf(-1).equals(redis.getExpire(SECKILL_TXN_KEY + orderId))) {
                    throw new SeckillUnavailableException("Invalid transaction state or TTL: " + orderId);
                }
            }
            return record;
        }
        catch (DataAccessException e) { throw new SeckillUnavailableException("Cannot read reservation", e); }
    }

    public VoucherOrder identity(Long orderId, Map<Object, Object> record) {
        try {
            if (!String.valueOf(orderId).equals(record.get("orderId"))) throw new IllegalArgumentException("orderId mismatch");
            VoucherOrder order = new VoucherOrder().setId(orderId)
                    .setUserId(Long.valueOf((String) record.get("userId")))
                    .setVoucherId(Long.valueOf((String) record.get("voucherId")));
            validateIdentity(order);
            return order;
        } catch (RuntimeException e) { throw new SeckillUnavailableException("Invalid transaction identity: " + orderId, e); }
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

    /** SCAN (not KEYS), isolated failure per record. Records remain until an explicit archival policy exists. */
    public void scanTransactions(BiConsumer<VoucherOrder, Map<Object, Object>> visitor) {
        redis.execute((RedisCallback<Void>) connection -> {
            try (Cursor<byte[]> cursor = connection.scan(ScanOptions.scanOptions().match(SECKILL_TXN_KEY + "*").count(100).build())) {
                while (cursor.hasNext()) {
                    String key = new String(cursor.next(), StandardCharsets.UTF_8);
                    try {
                        Long id = Long.valueOf(key.substring(SECKILL_TXN_KEY.length()));
                        Map<Object, Object> record = read(id);
                        if (!record.isEmpty()) visitor.accept(identity(id, record), record);
                    } catch (RuntimeException e) { log.error("SECKILL_RECORD_REVIEW: key={}", key, e); }
                }
            }
            return null;
        });
    }

    public static void validateIdentity(VoucherOrder order) {
        if (order == null || order.getId() == null || order.getId() <= 0
                || order.getUserId() == null || order.getUserId() <= 0
                || order.getVoucherId() == null || order.getVoucherId() <= 0) {
            throw new IllegalArgumentException("Incomplete order identity");
        }
    }
}
