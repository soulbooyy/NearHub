package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class OrderPersistenceResult {
    public enum Status { CREATED, ALREADY_CREATED, DUPLICATE_PURCHASE }

    private final Status status;
    private final VoucherOrder order;
}
