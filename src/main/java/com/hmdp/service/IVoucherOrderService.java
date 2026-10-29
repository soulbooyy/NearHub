package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result payOrder(Long orderId);

    /** 取消超时未支付订单（RocketMQ 延迟消息触发，回补库存） */
    void cancelTimeoutOrder(Long orderId);

    /** MQ 异步落库创建订单（幂等） */
    void createOrderFromMQ(VoucherOrder voucherOrder);

    /**
     * 查询秒杀异步结果
     * @return data 含 orderId、status
     */
    Result getSeckillResult(Long orderId);

    Result getSeckillResultByVoucher(Long voucherId);
}
