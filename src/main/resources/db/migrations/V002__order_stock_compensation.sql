-- Additive migration. Do not backfill historical cancellations: their refund state is unknown.
CREATE TABLE IF NOT EXISTS tb_order_stock_compensation (
    order_id BIGINT NOT NULL PRIMARY KEY,
    voucher_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    next_retry_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    lease_until DATETIME(3) NULL,
    lease_token VARCHAR(36) NULL,
    last_error VARCHAR(1000) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    INDEX idx_compensation_retry (status, next_retry_at),
    INDEX idx_compensation_lease (status, lease_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
