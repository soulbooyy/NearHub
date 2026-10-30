package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import lombok.Data;

/**
 * 秒杀事务消息上下文：半消息本地事务执行后回填 Lua 结果，供接口层返回错误原因
 */
@Data
public class SeckillTxContext {

    private final VoucherOrder order;
    /** Lua 返回码：0 成功；1 库存不足；2 重复下单；-1 未执行/异常 */
    private long luaResult = -1;

    public SeckillTxContext(VoucherOrder order) {
        this.order = order;
    }
}
