package com.hmdp.service;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

import static com.hmdp.service.OrderPersistenceResult.Status.*;
import static com.hmdp.utils.RedisConstants.ORDER_STATUS_UNPAID;

/** Only MySQL work belongs here. Constraints and rollback provide concurrency safety without an order lock. */
@Service
@RequiredArgsConstructor
public class VoucherOrderPersistenceService {
    private final VoucherOrderMapper orderMapper;
    private final SeckillVoucherMapper voucherMapper;

    @Transactional(rollbackFor = Exception.class)
    public OrderPersistenceResult createOrder(VoucherOrder message) {
        OrderPersistenceResult existing = findExisting(message);
        if (existing != null) {
            return existing;
        }
        if (voucherMapper.decreaseStock(message.getVoucherId()) != 1) {
            throw new StockRejectedException(message);
        }
        // Do not trust status/timestamps in a replayed MQ payload. Establish them once in DB.
        VoucherOrder order = new VoucherOrder();
        order.setId(message.getId());
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());
        order.setStatus(ORDER_STATUS_UNPAID);
        // Leave create_time unset: MySQL's CURRENT_TIMESTAMP default is authoritative.
        if (orderMapper.insert(order) != 1) {
            throw new IllegalStateException("Order insert affected no row: " + message.getId());
        }
        return new OrderPersistenceResult(CREATED, orderMapper.selectById(order.getId()));
    }

    /** Also called outside the failed transaction, AFTER its rollback, to classify a race. */
    public OrderPersistenceResult findExisting(VoucherOrder message) {
        VoucherOrder sameId = orderMapper.selectById(message.getId());
        if (sameId != null) {
            if (!Objects.equals(sameId.getUserId(), message.getUserId())
                    || !Objects.equals(sameId.getVoucherId(), message.getVoucherId())) {
                throw new IllegalStateException("Order ID conflicts with different business data: " + message.getId());
            }
            return new OrderPersistenceResult(ALREADY_CREATED, sameId);
        }
        VoucherOrder samePurchase = orderMapper.selectOne(Wrappers.<VoucherOrder>lambdaQuery()
                .eq(VoucherOrder::getUserId, message.getUserId())
                .eq(VoucherOrder::getVoucherId, message.getVoucherId()));
        if (samePurchase == null) return null;
        // Outside a transaction, the original order may commit between these two reads.
        return new OrderPersistenceResult(Objects.equals(samePurchase.getId(), message.getId())
                ? ALREADY_CREATED : DUPLICATE_PURCHASE, samePurchase);
    }
}
