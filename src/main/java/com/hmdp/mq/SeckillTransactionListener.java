package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.SeckillReservationService;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import static com.hmdp.service.SeckillReservationService.*;

@Slf4j
@Component
public class SeckillTransactionListener implements TransactionListener {
    @Resource private SeckillReservationService reservations;
    @Resource private VoucherOrderMapper orders;

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        SeckillTxContext context = arg instanceof SeckillTxContext ? (SeckillTxContext) arg : null;
        try {
            VoucherOrder order = decode(msg);
            if (context == null || !sameIdentity(order, context.getOrder())) {
                throw new IllegalArgumentException("Message/context identity mismatch");
            }
            long result = reservations.reserve(order);
            context.setLuaResult(result);
            return decision(result);
        } catch (Exception e) {
            if (context != null) context.setLuaResult(UNKNOWN);
            log.error("SECKILL_LOCAL_TRANSACTION_UNCERTAIN", e);
            return LocalTransactionState.UNKNOW;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        try {
            VoucherOrder order = decode(msg);
            // A committed DB order is authoritative even when the Redis record was lost.
            VoucherOrder persisted = orders.selectById(order.getId());
            if (persisted != null) {
                if (!sameIdentity(order, persisted)) throw new IllegalStateException("Order identity conflict");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            long result = reservations.check(order);
            if (result == UNKNOWN) log.warn("SECKILL_TRANSACTION_UNRESOLVED: orderId={}; inspect preparation/evidence", order.getId());
            return decision(result);
        }
        catch (Exception e) {
            log.error("SECKILL_TRANSACTION_CHECK_UNCERTAIN", e);
            return LocalTransactionState.UNKNOW;
        }
    }

    private VoucherOrder decode(Message message) {
        VoucherOrder order = JSONUtil.toBean(new String(message.getBody(), StandardCharsets.UTF_8), VoucherOrder.class);
        validateIdentity(order);
        return order;
    }

    private boolean sameIdentity(VoucherOrder a, VoucherOrder b) {
        return b != null && Objects.equals(a.getId(), b.getId()) && Objects.equals(a.getUserId(), b.getUserId())
                && Objects.equals(a.getVoucherId(), b.getVoucherId());
    }

    private LocalTransactionState decision(long result) {
        if (result == RESERVED) return LocalTransactionState.COMMIT_MESSAGE;
        if (result == NO_STOCK || result == DUPLICATE || result == ABORTED) return LocalTransactionState.ROLLBACK_MESSAGE;
        return LocalTransactionState.UNKNOW;
    }
}
