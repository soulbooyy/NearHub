package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.SeckillReservationService;
import org.springframework.beans.factory.annotation.Value;
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
    @Value("${seckill.transaction.check-grace-ms:60000}") private long checkGraceMs = 60000;

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
            if (reservations.hasSuccessMarker(order.getId())) return LocalTransactionState.COMMIT_MESSAGE;
            long born = msg.getBornTimestamp();
            long now = System.currentTimeMillis();
            // A bounded grace uses the original MQ timestamp; no persisted preparation state.
            // It reduces early checks, but does not fence a callback that resumes after rollback.
            if (born <= 0 || born > now || now - born < checkGraceMs) return LocalTransactionState.UNKNOW;
            log.warn("SECKILL_MARKER_MISSING_ROLLBACK: orderId={}, ageMs={}; expiry/loss can hide a successful reservation",
                    order.getId(), now - born);
            return LocalTransactionState.ROLLBACK_MESSAGE;
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
        if (result == SUCCESS) return LocalTransactionState.COMMIT_MESSAGE;
        if (result == NO_STOCK || result == DUPLICATE) return LocalTransactionState.ROLLBACK_MESSAGE;
        return LocalTransactionState.UNKNOW;
    }
}
