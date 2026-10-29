package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.SeckillRecoveryTask;
import org.apache.ibatis.annotations.*;

import java.util.List;

public interface SeckillRecoveryTaskMapper extends BaseMapper<SeckillRecoveryTask> {
    String DUE = "((status='PENDING' AND next_retry_at<=CURRENT_TIMESTAMP(3)) "
            + "OR (status='PROCESSING' AND lease_until<=CURRENT_TIMESTAMP(3)))";
    String OWNED = "order_id=#{orderId} AND status='PROCESSING' AND lease_token=#{token} "
            + "AND lease_until>CURRENT_TIMESTAMP(3)";

    @Select("SELECT order_id FROM tb_seckill_recovery_task WHERE " + DUE
            + " ORDER BY next_retry_at,order_id LIMIT #{limit}")
    List<Long> findDueIds(@Param("limit") int limit);

    @Insert("INSERT INTO tb_seckill_recovery_task(order_id,voucher_id,user_id) VALUES(#{id},#{voucherId},#{userId}) "
            + "ON DUPLICATE KEY UPDATE order_id=order_id")
    int register(com.hmdp.entity.VoucherOrder order);

    // Claiming for confirmation does not consume the resend budget. Sending consumes it BEFORE IO.
    @Update("UPDATE tb_seckill_recovery_task SET status='PROCESSING', "
            + "lease_token=#{token}, lease_until=DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL #{leaseSeconds} SECOND) "
            + "WHERE order_id=#{orderId} AND " + DUE)
    int claim(@Param("orderId") Long orderId, @Param("token") String token, @Param("leaseSeconds") int leaseSeconds);

    @Update("UPDATE tb_seckill_recovery_task SET attempts=attempts+1 WHERE " + OWNED + " AND attempts<#{maxAttempts}")
    int beginAttempt(@Param("orderId") Long orderId, @Param("token") String token, @Param("maxAttempts") int maxAttempts);

    @Update("UPDATE tb_seckill_recovery_task SET status='DONE', last_error=NULL, lease_token=NULL, lease_until=NULL WHERE " + OWNED)
    int complete(@Param("orderId") Long orderId, @Param("token") String token);

    @Update("UPDATE tb_seckill_recovery_task SET status=#{status}, last_error=#{error}, "
            + "next_retry_at=DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL #{delaySeconds} SECOND), "
            + "lease_token=NULL, lease_until=NULL WHERE " + OWNED)
    int fail(@Param("orderId") Long orderId, @Param("token") String token, @Param("status") String status,
             @Param("error") String error, @Param("delaySeconds") int delaySeconds);
}
