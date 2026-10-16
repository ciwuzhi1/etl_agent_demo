CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.clean_orders (
    order_id BIGINT NOT NULL,
    customer_name VARCHAR(100),
    amount DECIMAL(12, 2),
    status VARCHAR(20),
    email VARCHAR(255),
    created_at DATETIME,
    source_system VARCHAR(32)
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS analytics.rejected_orders (
    order_id BIGINT NOT NULL,
    customer_name VARCHAR(100),
    amount DECIMAL(12, 2),
    status VARCHAR(20),
    email VARCHAR(255),
    created_at DATETIME,
    source_system VARCHAR(32),
    error_reason VARCHAR(64)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS analytics.file_clean_orders (
    order_id BIGINT NOT NULL,
    customer_name VARCHAR(100),
    amount DOUBLE,
    status VARCHAR(20),
    email VARCHAR(255),
    created_at DATE,
    source_system VARCHAR(32)
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
