package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("tb_order_stock_compensation")
public class OrderStockCompensation {
    public static final String PENDING = "PENDING";
    public static final String PROCESSING = "PROCESSING";
    public static final String DONE = "DONE";
    public static final String MANUAL_REVIEW = "MANUAL_REVIEW";

    @TableId(type = IdType.INPUT)
    private Long orderId;
    private Long voucherId;
    private Long userId;
    private String status;
    private Integer attempts;
    private LocalDateTime nextRetryAt;
    private LocalDateTime leaseUntil;
    private String leaseToken;
    private String lastError;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
