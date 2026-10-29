package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import java.util.List;
import java.time.LocalDateTime;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {
    @Select("SELECT CURRENT_TIMESTAMP")
    LocalDateTime databaseNow();

    @Update("UPDATE tb_voucher_order SET status=4 WHERE id=#{orderId} AND status=1 "
            + "AND create_time<=DATE_SUB(CURRENT_TIMESTAMP, INTERVAL #{minutes} MINUTE)")
    int cancelExpired(@Param("orderId") Long orderId, @Param("minutes") int minutes);

    @Select("SELECT * FROM tb_voucher_order WHERE status=1 "
            + "AND create_time<=DATE_SUB(CURRENT_TIMESTAMP, INTERVAL #{minutes} MINUTE) ORDER BY create_time,id LIMIT #{limit}")
    List<VoucherOrder> findExpired(@Param("minutes") int minutes, @Param("limit") int limit);
}
