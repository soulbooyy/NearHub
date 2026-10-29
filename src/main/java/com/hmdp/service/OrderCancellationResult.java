package com.hmdp.service;

import com.hmdp.entity.VoucherOrder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class OrderCancellationResult {
    public enum Status { CANCELLED, ALREADY_CANCELLED, NOT_DUE, IGNORED, LEGACY_REVIEW }
    private final Status status;
    private final VoucherOrder order;
}
