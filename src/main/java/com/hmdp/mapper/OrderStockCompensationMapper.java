package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.OrderStockCompensation;
import org.apache.ibatis.annotations.*;

import java.util.List;

public interface OrderStockCompensationMapper extends BaseMapper<OrderStockCompensation> {
    String DUE = "((status='PENDING' AND next_retry_at<=CURRENT_TIMESTAMP(3)) "
            + "OR (status='PROCESSING' AND lease_until<=CURRENT_TIMESTAMP(3)))";
    String OWNED = "order_id=#{orderId} AND status='PROCESSING' AND lease_token=#{token} "
            + "AND lease_until>CURRENT_TIMESTAMP(3)";

    @Select("SELECT order_id FROM tb_order_stock_compensation WHERE " + DUE
            + " ORDER BY next_retry_at,order_id LIMIT #{limit}")
    List<Long> findDueIds(@Param("limit") int limit);

    @Update("UPDATE tb_order_stock_compensation SET status='PROCESSING', attempts=attempts+1, "
            + "lease_token=#{token}, lease_until=DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL #{leaseSeconds} SECOND) "
            + "WHERE order_id=#{orderId} AND attempts<#{maxAttempts} AND " + DUE)
    int claim(@Param("orderId") Long orderId, @Param("token") String token,
              @Param("leaseSeconds") int leaseSeconds, @Param("maxAttempts") int maxAttempts);

    @Update("UPDATE tb_order_stock_compensation SET status='MANUAL_REVIEW', lease_token=NULL, lease_until=NULL, "
            + "last_error='Retry budget exhausted after worker interruption' "
            + "WHERE order_id=#{orderId} AND attempts>=#{maxAttempts} AND " + DUE)
    int exhaust(@Param("orderId") Long orderId, @Param("maxAttempts") int maxAttempts);

    @Update("UPDATE tb_order_stock_compensation SET status='DONE', last_error=NULL, lease_token=NULL, lease_until=NULL WHERE " + OWNED)
    int complete(@Param("orderId") Long orderId, @Param("token") String token);

    @Update("UPDATE tb_order_stock_compensation SET status=#{status}, last_error=#{error}, "
            + "next_retry_at=DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL #{delaySeconds} SECOND), "
            + "lease_token=NULL, lease_until=NULL WHERE " + OWNED)
    int fail(@Param("orderId") Long orderId, @Param("token") String token, @Param("status") String status,
             @Param("error") String error, @Param("delaySeconds") int delaySeconds);
}
