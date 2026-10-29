package com.hmdp.event;

/** In-process invalidation hint; deliberately not a durable event. */
public final class ShopUpdatedEvent {
    private final Long shopId;

    public ShopUpdatedEvent(Long shopId) { this.shopId = shopId; }
    public Long getShopId() { return shopId; }
}
