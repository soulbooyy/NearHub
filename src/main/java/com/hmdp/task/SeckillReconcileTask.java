package com.hmdp.task;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.OrderTimeoutPolicy;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 超时关单兜底、缺单告警与只读库存核对；不自动重投或修复缺单。
 * 库存差异只告警，禁止自动 SET/UPDATE 覆盖预占或重复计算回补。
 */
@Slf4j
@Component
public class SeckillReconcileTask {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private VoucherOrderMapper orders;

    /** 固定业务期限；延迟消息档位只负责唤醒，不能决定是否到期。 */
    private static final int ORDER_TIMEOUT_MINUTES = OrderTimeoutPolicy.TIMEOUT_MINUTES;
    /** 只读核查等待时间；不是 MQ 已排空的证明。 */
    private static final int RECONCILE_AFTER_END_MINUTES = 2;

    /**
     * 每分钟执行：关单兜底 → 缺单核查 → 库存差异告警。
     * 多实例部署时用分布式锁保证同一时刻只有一个实例在对账；不代表与消费端已实现全链路互斥。
     */
    @Scheduled(fixedDelay = 60_000)
    public void reconcile() {
        RLock lock = redissonClient.getLock("lock:seckill:reconcile");
        if (!lock.tryLock()) {
            return; // 其他实例正在对账
        }
        try {
            closeTimeoutOrders();
            Set<Long> unresolved = reportMissingOrders();
            auditFinishedStocks(unresolved);
        } catch (Exception e) {
            log.error("秒杀对账任务执行异常", e);
        } finally {
            if (lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    /**
     * ① 关单兜底：兜 TIMEOUT 延迟消息丢失。
     * 复用 cancelTimeoutOrder（CAS: WHERE status=未支付），与延迟消息并发执行也不重复回补库存。
     */
    private void closeTimeoutOrders() {
        List<VoucherOrder> expired = orders.findExpired(ORDER_TIMEOUT_MINUTES, 100);
        for (VoucherOrder order : expired) {
            try {
                voucherOrderService.cancelTimeoutOrder(order.getId());
            } catch (Exception e) {
                log.error("Order cancellation will be retried on next scan: {}", order.getId(), e);
            }
        }
        if (!expired.isEmpty()) {
            // expired.size() 是扫描数，CAS 会跳过已支付/已取消的，实际关闭数 ≤ 扫描数
            log.warn("对账关单兜底：扫描到 {} 笔超时未支付订单（CAS 幂等，已处理过的自动跳过）", expired.size());
        }
    }

    /** After the campaign ends plus a grace, report claims without orders for manual investigation. */
    private Set<Long> reportMissingOrders() {
        Set<Long> unresolved = new HashSet<>();
        List<SeckillVoucher> vouchers = seckillVoucherService.lambdaQuery()
                .lt(SeckillVoucher::getEndTime, LocalDateTime.now().minusMinutes(RECONCILE_AFTER_END_MINUTES))
                .list();
        for (SeckillVoucher voucher : vouchers) {
            Long voucherId = voucher.getVoucherId();
            Set<String> claimed = stringRedisTemplate.opsForSet()
                    .members(RedisConstants.SECKILL_ORDER_KEY + voucherId);
            if (claimed == null || claimed.isEmpty()) {
                continue;
            }
            List<VoucherOrder> created = voucherOrderService.lambdaQuery()
                    .select(VoucherOrder::getUserId)
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .list();
            Set<String> createdSet = created.stream()
                    .map(o -> String.valueOf(o.getUserId()))
                    .collect(Collectors.toSet());

            for (String userId : claimed) {
                if (createdSet.contains(userId)) {
                    continue;
                }
                unresolved.add(voucherId);
                Object owner = stringRedisTemplate.opsForHash().get(
                        RedisConstants.SECKILL_CLAIM_OWNER_KEY + voucherId, userId);
                log.error("ORDER_MISSING_AUDIT: reservation without order, voucherId={}, userId={}, originalOrderId={}; inspect ownership/stock/DLQ manually, do not create a new orderId",
                        voucherId, userId, owner);
            }
        }
        return unresolved;
    }

    /** Diagnostic estimate only: no stock writes, even when no unresolved claims were observed. */
    private void auditFinishedStocks(Set<Long> unresolved) {
        List<SeckillVoucher> finished = seckillVoucherService.lambdaQuery()
                .lt(SeckillVoucher::getEndTime, LocalDateTime.now().minusMinutes(RECONCILE_AFTER_END_MINUTES))
                .list();
        for (SeckillVoucher voucher : finished) {
            Long voucherId = voucher.getVoucherId();
            if (unresolved.contains(voucherId)) {
                continue;
            }
            int initial = voucher.getInitialStock() == null ? 0 : voucher.getInitialStock();
            if (initial <= 0) {
                // 无账本基准（存量数据未回填/发布时未赋值），跳过估算
                log.warn("库存差异核查跳过：voucherId={} 缺少 initialStock，请确认是否执行回填 SQL", voucherId);
                continue;
            }
            long valid = voucherOrderService.lambdaQuery()
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .ne(VoucherOrder::getStatus, RedisConstants.ORDER_STATUS_CANCELLED)
                    .count();
            int expected = Math.max(0, initial - (int) valid);

            boolean dbOk = voucher.getStock() != null && voucher.getStock() == expected;
            String redisStock = stringRedisTemplate.opsForValue().get(SECKILL_STOCK_KEY + voucherId);
            boolean redisOk = String.valueOf(expected).equals(redisStock);
            if (dbOk && redisOk) {
                continue; // 两边与估算一致，无差异告警
            }
            log.warn("STOCK_AUDIT_ONLY: voucherId={}, db={}, redis={}, expectedEstimate={}; no automatic repair",
                    voucherId, voucher.getStock(), redisStock, expected);
        }
    }
}
