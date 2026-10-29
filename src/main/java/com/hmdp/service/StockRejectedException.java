package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;

/** Redis 已预占但 DB 无库存：保留预占，有限重试后进入 DLQ，需人工核账。 */
public class StockRejectedException extends RuntimeException {
    public StockRejectedException(VoucherOrder order) {
        super("STOCK_REJECTED: orderId=" + order.getId() + ", userId=" + order.getUserId()
                + ", voucherId=" + order.getVoucherId() + "; reservation retained; review DLQ if retries exhausted");
    }
}
