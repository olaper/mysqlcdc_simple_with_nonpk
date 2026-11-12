-- =====================================================================
-- Mixed-key CDC test script. Matches the explicit keys in cdc_sync.py:
--   PRIMARY_KEY: product(product_id), order_item(order_id, line_no)
--   UNIQUE_KEY (no PK): customer(customer_no), orders(order_no)
-- Execute each STEP separately, NOT the entire file in one run.
-- =====================================================================


-- ---------------------------------------------------------------------
-- STEP 1. Create tables  (run on BOTH source and target)
--   source: gphinblue-poc.mysql.database.azure.com
--   target: gphingreen-poc.mysql.database.azure.com
--   Note: if you get "unable to create a table without a primary key",
--   set the server parameter sql_require_primary_key = OFF.
--   MySQL 8.0.30+: disable sql_generate_invisible_primary_key if enabled,
--   otherwise the "no PK" tables may receive an invisible generated PK.
--   Source must use binlog_format=ROW and binlog_row_image=FULL.
--   WARNING: this step DROPS sample tables and their existing data.
--   Stop CDC before setup. Use a fresh checkpoint for a new empty-table test;
--   do not reuse cdc_state.json from an earlier test/database.
--   The database name below must match DB in .env.
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ghbin_sampledb2;
USE ghbin_sampledb2;

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
    customer_no  VARCHAR(20)  NOT NULL,      -- unique key (used instead of a PK)
    name         VARCHAR(100) NOT NULL,
    email        VARCHAR(200) NULL,
    grade        VARCHAR(10)  NULL,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_customer_no (customer_no)  -- required on the target for ON DUPLICATE KEY
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_no     VARCHAR(30)   NOT NULL,     -- unique key (used instead of a PK)
    customer_no  VARCHAR(20)   NOT NULL,
    amount       DECIMAL(12,2) NOT NULL,
    status       VARCHAR(20)   NOT NULL DEFAULT 'NEW',
    ordered_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_order_no (order_no)
);

DROP TABLE IF EXISTS product;
CREATE TABLE product (
    product_id   BIGINT        NOT NULL,
    name         VARCHAR(100)  NOT NULL,
    price        DECIMAL(12,2) NOT NULL,
    PRIMARY KEY (product_id)
);

DROP TABLE IF EXISTS order_item;
CREATE TABLE order_item (
    order_id     BIGINT        NOT NULL,
    line_no      INT           NOT NULL,
    product_id   BIGINT        NOT NULL,
    quantity     INT           NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Confirm PRIMARY appears only for product and order_item.
SHOW INDEX FROM customer;
SHOW INDEX FROM orders;
SHOW INDEX FROM product;
SHOW INDEX FROM order_item;


-- ---------------------------------------------------------------------
-- STEP 2. Start cdc_sync.py, then run these INSERTs on SOURCE ONLY.
--   Wait for "Starting position" and leave CDC running through STEP 3.
--   Both servers must have empty tables from STEP 1 before starting.
--   No snapshot copy is needed: all sample rows are inserted after CDC starts.
--   Existing production data requires a separate consistent snapshot/bootstrap.
-- ---------------------------------------------------------------------
INSERT INTO customer (customer_no, name, email, grade) VALUES
    ('C001', 'Alice', 'alice@example.com', 'GOLD'),
    ('C002', 'Bob',   'bob@example.com',   'SILVER');

INSERT INTO orders (order_no, customer_no, amount, status) VALUES
    ('O1001', 'C001', 150.00, 'NEW'),
    ('O1002', 'C002',  75.50, 'NEW');

INSERT INTO product (product_id, name, price) VALUES
    (101, 'Keyboard', 50.00),
    (102, 'Mouse', 25.00);

INSERT INTO order_item (order_id, line_no, product_id, quantity) VALUES
    (1001, 1, 101, 2),
    (1001, 2, 102, 1),
    (1002, 1, 102, 3);


-- ---------------------------------------------------------------------
-- STEP 3. Generate changes  (run on SOURCE while cdc_sync.py is running)
-- ---------------------------------------------------------------------

-- 3-1. INSERT
INSERT INTO customer (customer_no, name, email, grade)
VALUES ('C003', 'Carol', 'carol@example.com', 'BRONZE');

INSERT INTO orders (order_no, customer_no, amount, status)
VALUES ('O1003', 'C003', 20.00, 'NEW');

INSERT INTO product (product_id, name, price)
VALUES (103, 'Temporary product', 10.00);

INSERT INTO order_item (order_id, line_no, product_id, quantity)
VALUES (1003, 1, 103, 1);

-- 3-2. UPDATE (normal column)
UPDATE customer SET grade = 'PLATINUM' WHERE customer_no = 'C001';
UPDATE orders   SET status = 'PAID', amount = 160.00 WHERE order_no = 'O1001';
UPDATE product SET price = 55.00 WHERE product_id = 101;
UPDATE order_item SET quantity = 4 WHERE order_id = 1001 AND line_no = 1;

-- 3-3. UPDATE of the unique column itself (the script uses the "before" value to find the row)
UPDATE customer SET customer_no = 'C002X' WHERE customer_no = 'C002';
UPDATE orders SET order_no = 'O1002X' WHERE order_no = 'O1002';

-- Change single and composite PK values: CDC locates rows by the OLD key.
UPDATE product SET product_id = 202 WHERE product_id = 102;
UPDATE order_item SET line_no = 3 WHERE order_id = 1001 AND line_no = 2;

-- 3-4. Multi-row UPDATE (one event carrying several rows)
UPDATE orders SET status = 'SHIPPED' WHERE status IN ('NEW', 'PAID');
UPDATE order_item SET quantity = quantity + 1 WHERE order_id = 1001;

-- 3-5. DELETE
DELETE FROM orders   WHERE order_no = 'O1003';
DELETE FROM customer WHERE customer_no = 'C003';
DELETE FROM order_item WHERE order_id = 1003 AND line_no = 1;
DELETE FROM product WHERE product_id = 103;

-- No FKs are defined: changing a customer/product key does not cascade
-- to orders/order_item; the same independent row images are copied to target.


-- ---------------------------------------------------------------------
-- STEP 4. Verify (after CDC catches up, run on BOTH servers).
--   Full row results should match. Expected counts: 2, 2, 2, 3.
-- ---------------------------------------------------------------------
SELECT * FROM customer ORDER BY customer_no;
SELECT * FROM orders   ORDER BY order_no;
SELECT * FROM product ORDER BY product_id;
SELECT * FROM order_item ORDER BY order_id, line_no;

-- Quick count / checksum comparison
SELECT COUNT(*) AS cnt FROM customer;
SELECT COUNT(*) AS cnt FROM orders;
SELECT COUNT(*) AS cnt FROM product;
SELECT COUNT(*) AS cnt FROM order_item;
CHECKSUM TABLE customer, orders, product, order_item;

-- Expected product IDs: 101, 202; product 101 price: 55.00.
-- Expected order_item keys/quantities: (1001,1)=5, (1001,3)=2, (1002,1)=3.
-- Expected customer keys: C001, C002X; C001 grade: PLATINUM.
-- Expected order keys: O1001, O1002X; both status: SHIPPED.


-- ---------------------------------------------------------------------
-- Optional: grants (adjust host/password as needed)
-- ---------------------------------------------------------------------
-- On SOURCE (cdc_reader):
--   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%';
--   GRANT SELECT ON ghbin_sampledb2.* TO 'cdc_reader'@'%';
-- On TARGET (cdc_writer):
--   GRANT SELECT, INSERT, UPDATE, DELETE ON ghbin_sampledb2.* TO 'cdc_writer'@'%';
