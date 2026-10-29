package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 秒杀异步结果查询
     */
    @GetMapping("seckill/result/{orderId}")
    public Result seckillResult(@PathVariable("orderId") Long orderId) {
        return voucherOrderService.getSeckillResult(orderId);
    }

    @GetMapping("seckill/result/by-voucher/{voucherId}")
    public Result seckillResultByVoucher(@PathVariable("voucherId") Long voucherId) {
        return voucherOrderService.getSeckillResultByVoucher(voucherId);
    }

    /**
     * 模拟支付
     */
    @PutMapping("pay/{id}")
    public Result payOrder(@PathVariable("id") Long orderId) {
        return voucherOrderService.payOrder(orderId);
    }
}
