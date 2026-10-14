CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    customer_name VARCHAR(100),
    amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    email VARCHAR(255),
    created_at DATETIME NOT NULL
);

INSERT INTO orders (order_id, customer_name, amount, status, email, created_at) VALUES
    (1001, ' Alice ', 199.90, 'PAID', 'alice@example.com', '2026-08-01 09:00:00'),
    (1002, 'Bob', 88.00, 'PAID', 'bob@example.com', '2026-08-01 10:00:00'),
    (1003, 'Carol', 520.35, 'PAID', 'carol@example.com', '2026-08-02 11:30:00'),
    (1004, 'David', 45.60, 'PAID', 'david@example.com', '2026-08-03 14:20:00'),
    (1005, 'Eve', 12.00, 'CANCELLED', 'eve@example.com', '2026-08-03 15:00:00'),
    (1006, 'Frank', -9.99, 'PAID', 'frank@example.com', '2026-08-03 16:00:00'),
    (1007, '', 66.00, 'PAID', 'empty@example.com', '2026-08-04 08:00:00'),
    (1008, NULL, 77.00, 'PAID', 'null@example.com', '2026-08-04 09:00:00')
ON DUPLICATE KEY UPDATE order_id = VALUES(order_id);
