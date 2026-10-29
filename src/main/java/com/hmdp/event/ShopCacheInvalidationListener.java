package com.hmdp.event;

import com.hmdp.utils.MultiLevelCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShopCacheInvalidationListener {
    private final MultiLevelCacheService cache;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onShopUpdated(ShopUpdatedEvent event) {
        try {
            cache.evict(CACHE_SHOP_KEY, event.getShopId());
        } catch (RuntimeException e) {
            // The DB has already committed. Do not report that the business update rolled back.
            // This best-effort hook can be lost on process death; TTL/rebuild is the fallback.
            log.error("SHOP_CACHE_INVALIDATION_FAILED: shopId={}; database update already committed",
                    event.getShopId(), e);
        }
    }
}
