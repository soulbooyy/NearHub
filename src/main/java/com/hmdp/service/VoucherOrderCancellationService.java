package com.hmdp.service;

import com.hmdp.entity.OrderStockCompensation;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.OrderStockCompensationMapper;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.utils.OrderTimeoutPolicy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static com.hmdp.service.OrderCancellationResult.Status.*;
import static com.hmdp.utils.RedisConstants.*;

/** Redis never participates in this transaction. The task is the durable recovery obligation. */
@Service
@RequiredArgsConstructor
public class VoucherOrderCancellationService {
    private final VoucherOrderMapper orders;
    private final SeckillVoucherMapper vouchers;
    private final OrderStockCompensationMapper tasks;

    @Transactional(rollbackFor = Exception.class)
    public OrderCancellationResult cancelExpired(Long orderId) {
        if (orderId == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        int changed = orders.cancelExpired(orderId, OrderTimeoutPolicy.TIMEOUT_MINUTES);
        VoucherOrder order = orders.selectById(orderId);
        if (changed == 1) {
            if (vouchers.restoreStock(order.getVoucherId()) != 1) {
                throw new IllegalStateException("Cannot restore MySQL stock for order " + orderId);
            }
            OrderStockCompensation task = new OrderStockCompensation();
            task.setOrderId(orderId);
            task.setVoucherId(order.getVoucherId());
            task.setUserId(order.getUserId());
            task.setStatus(OrderStockCompensation.PENDING);
            if (tasks.insert(task) != 1) {
                throw new IllegalStateException("Cannot persist stock compensation for order " + orderId);
            }
            return new OrderCancellationResult(CANCELLED, order);
        }
        if (order == null) {
            return new OrderCancellationResult(IGNORED, null);
        }
        if (Integer.valueOf(ORDER_STATUS_CANCELLED).equals(order.getStatus())) {
            return new OrderCancellationResult(tasks.selectById(orderId) == null ? LEGACY_REVIEW : ALREADY_CANCELLED, order);
        }
        return new OrderCancellationResult(Integer.valueOf(ORDER_STATUS_UNPAID).equals(order.getStatus())
                ? NOT_DUE : IGNORED, order);
    }
}
