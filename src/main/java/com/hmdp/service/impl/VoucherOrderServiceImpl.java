package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.RocketMQProducer;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.*;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.DataAccessException;
import java.util.Objects;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.service.SeckillReservationService.*;

/** Single transactional-message seckill flow. No order-level distributed lock. */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource private RedisIdWorker redisIdWorker;
    @Resource private RocketMQProducer rocketMQProducer;
    @Resource private VoucherOrderPersistenceService persistenceService;
    @Resource private VoucherOrderCancellationService cancellationService;
    @Resource private SeckillReservationService reservations;

    @Override
    public void cancelTimeoutOrder(Long orderId) {
        OrderCancellationResult result = cancellationService.cancelExpired(orderId);
        if (result.getStatus() == OrderCancellationResult.Status.NOT_DUE) {
            sendTimeoutOrRetry(result.getOrder());
        } else if (result.getStatus() == OrderCancellationResult.Status.LEGACY_REVIEW) {
            log.error("STOCK_COMPENSATION_MANUAL_REVIEW: cancelled order {} has no task", orderId);
        }
    }

    @Override
    public Result payOrder(Long orderId) {
        boolean updated = update().set("status", ORDER_STATUS_PAID).set("pay_time", LocalDateTime.now())
                .eq("id", orderId).eq("user_id", UserHolder.getUser().getId())
                .eq("status", ORDER_STATUS_UNPAID).update();
        return updated ? Result.ok("支付成功") : Result.fail("订单不存在或状态不允许支付");
    }

    @Override
    public void createOrderFromMQ(VoucherOrder message) {
        validateIdentity(message);
        OrderPersistenceResult result = persistenceService.findExisting(message);
        if (result == null) {
            // Permanent ownership survives marker expiry. Never reserve or deduct Redis stock here.
            if (!Objects.equals(message.getId(), reservations.findOriginalOrderId(message.getVoucherId(), message.getUserId()))) {
                throw new IllegalStateException("CREATE without confirmed reservation: " + message.getId());
            }
            try {
                result = persistenceService.createOrder(message);
            } catch (DuplicateKeyException | StockRejectedException e) {
                // The independent Spring transaction has rolled back before this fresh read.
                result = persistenceService.findExisting(message);
                if (result == null) throw e;
            }
        }
        if (result.getStatus() == OrderPersistenceResult.Status.DUPLICATE_PURCHASE) {
            throw new IllegalStateException("Conflicting original order: " + message.getId()
                    + ", existing=" + result.getOrder().getId());
        }
        if (Integer.valueOf(ORDER_STATUS_UNPAID).equals(result.getOrder().getStatus())) {
            sendTimeoutOrRetry(result.getOrder());
        }
    }

    private void sendTimeoutOrRetry(VoucherOrder order) {
        try { rocketMQProducer.sendOrderTimeout(order); }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Timeout send interrupted: " + order.getId(), e);
        } catch (Exception e) { throw new IllegalStateException("Timeout send failed: " + order.getId(), e); }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = new VoucherOrder().setId(redisIdWorker.nextId("order"))
                .setUserId(userId).setVoucherId(voucherId);
        SeckillTxContext context = new SeckillTxContext(order);
        try {
            SendResult sent = rocketMQProducer.sendOrderCreateInTransaction(order, context);
            if (sent == null || sent.getSendStatus() != SendStatus.SEND_OK) {
                throw new SeckillPendingException(order.getId(), null);
            }
        } catch (Exception e) {
            log.error("SECKILL_SEND_UNCERTAIN: orderId={}", order.getId(), e);
            throw new SeckillPendingException(order.getId(), e);
        }
        long result = context.getLuaResult();
        if (result == SUCCESS) return Result.ok(order.getId());
        if (result == NO_STOCK) return Result.fail("库存不足");
        if (result == DUPLICATE) return Result.fail("不能重复下单");
        throw new SeckillPendingException(order.getId(), null);
    }

    @Override
    public Result getSeckillResult(Long orderId) {
        if (orderId == null || orderId <= 0) return Result.fail("订单号无效");
        try { return queryResult(orderId, null, false); }
        catch (DataAccessException e) { throw new SeckillUnavailableException("Cannot query order", e); }
    }

    @Override
    public Result getSeckillResultByVoucher(Long voucherId) {
        if (voucherId == null || voucherId <= 0) return Result.fail("券 ID 无效");
        Long userId = UserHolder.getUser().getId();
        try {
            VoucherOrder existing = lambdaQuery().eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getVoucherId, voucherId).one();
            if (existing != null) return queryResult(existing.getId(), voucherId, true);
            Long original = reservations.findOriginalOrderId(voucherId, userId);
            if (original != null) return queryResult(original, voucherId, true);
            Map<String, Object> data = new HashMap<>();
            data.put("voucherId", voucherId); data.put("status", "NOT_FOUND");
            return Result.ok(data);
        } catch (DataAccessException e) { throw new SeckillUnavailableException("Cannot find original order", e); }
    }

    private Result queryResult(Long orderId, Long expectedVoucher, boolean knownOwner) {
        Long userId = UserHolder.getUser().getId();
        Map<String, Object> data = new HashMap<>();
        data.put("orderId", orderId);
        VoucherOrder order = getById(orderId);
        if (order != null) {
            if (!Objects.equals(order.getUserId(), userId)) return Result.fail("订单不存在或无权访问");
            if (expectedVoucher != null && !Objects.equals(expectedVoucher, order.getVoucherId())) {
                throw new SeckillUnavailableException("Original order voucher mismatch");
            }
            data.put("status", "SUCCESS"); data.put("orderStatus", order.getStatus());
            return Result.ok(data);
        }
        // A plain success marker carries no user identity. Without a DB order or an authenticated
        // voucher/owner lookup, do not reveal whether somebody else's order was reserved.
        data.put("status", knownOwner && reservations.hasSuccessMarker(orderId) ? "PROCESSING" : "UNKNOWN");
        return Result.ok(data);
    }
}
