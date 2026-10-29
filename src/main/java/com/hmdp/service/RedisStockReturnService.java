package com.hmdp.service;

import com.hmdp.entity.OrderStockCompensation;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import static com.hmdp.utils.RedisConstants.*;

@Service
@RequiredArgsConstructor
public class RedisStockReturnService {
    private final StringRedisTemplate redis;
    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>();
    static {
        SCRIPT.setLocation(new ClassPathResource("return-order-stock.lua"));
        SCRIPT.setResultType(Long.class);
    }

    /** 0 applied, 1 already applied, 2/3/4 require manual review; null/exception is uncertain. */
    public Long returnStock(OrderStockCompensation task) {
        return redis.execute(SCRIPT, Arrays.asList(
                        SECKILL_STOCK_KEY + task.getVoucherId(),
                        SECKILL_CLAIM_OWNER_KEY + task.getVoucherId(),
                        SECKILL_ORDER_KEY + task.getVoucherId(),
                        SECKILL_STOCK_RETURNED_KEY + task.getOrderId()),
                task.getUserId().toString(), task.getOrderId().toString(), task.getVoucherId().toString());
    }
}
