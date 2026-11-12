-- =====================================================================
-- CDC test script (tables WITHOUT a primary key, WITH a unique column)
-- Matches UNIQUE_KEY in cdc_sync.py: customer -> customer_no, orders -> order_no
-- =====================================================================


-- ---------------------------------------------------------------------
-- STEP 1. Create tables  (run on BOTH source and target)
--   source: gphinblue-poc.mysql.database.azure.com
--   target: gphingreen-poc.mysql.database.azure.com
--   Note: if you get "unable to create a table without a primary key",
--   set the server parameter sql_require_primary_key = OFF.
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


-- ---------------------------------------------------------------------
-- STEP 2. (Optional) Initial data  (run on SOURCE only, BEFORE the initial load)
--   For a real test: insert these, copy them to the target (mysqldump),
--   then start cdc_sync.py from the position right after the dump.
-- ---------------------------------------------------------------------
INSERT INTO customer (customer_no, name, email, grade) VALUES
    ('C001', 'Alice', 'alice@example.com', 'GOLD'),
    ('C002', 'Bob',   'bob@example.com',   'SILVER');

INSERT INTO orders (order_no, customer_no, amount, status) VALUES
    ('O1001', 'C001', 150.00, 'NEW'),
    ('O1002', 'C002',  75.50, 'NEW');


-- ---------------------------------------------------------------------
-- STEP 3. Generate changes  (run on SOURCE while cdc_sync.py is running)
-- ---------------------------------------------------------------------

-- 3-1. INSERT
INSERT INTO customer (customer_no, name, email, grade)
VALUES ('C003', 'Carol', 'carol@example.com', 'BRONZE');

INSERT INTO orders (order_no, customer_no, amount, status)
VALUES ('O1003', 'C003', 20.00, 'NEW');

-- 3-2. UPDATE (normal column)
UPDATE customer SET grade = 'PLATINUM' WHERE customer_no = 'C001';
UPDATE orders   SET status = 'PAID', amount = 160.00 WHERE order_no = 'O1001';

-- 3-3. UPDATE of the unique column itself (the script uses the "before" value to find the row)
UPDATE customer SET customer_no = 'C002X' WHERE customer_no = 'C002';

-- 3-4. Multi-row UPDATE (one event carrying several rows)
UPDATE orders SET status = 'SHIPPED' WHERE status IN ('NEW', 'PAID');

-- 3-5. DELETE
DELETE FROM orders   WHERE order_no = 'O1003';
DELETE FROM customer WHERE customer_no = 'C003';


-- ---------------------------------------------------------------------
-- STEP 4. Verify  (run on BOTH source and target, results should match)
-- ---------------------------------------------------------------------
SELECT * FROM customer ORDER BY customer_no;
SELECT * FROM orders   ORDER BY order_no;

-- Quick count / checksum comparison
SELECT COUNT(*) AS cnt FROM customer;
SELECT COUNT(*) AS cnt FROM orders;
CHECKSUM TABLE customer, orders;


-- ---------------------------------------------------------------------
-- Optional: grants (adjust host/password as needed)
-- ---------------------------------------------------------------------
-- On SOURCE (cdc_reader):
--   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%';
--   GRANT SELECT ON ghbin_sampledb.* TO 'cdc_reader'@'%';
-- On TARGET (cdc_writer):
--   GRANT SELECT, INSERT, UPDATE, DELETE ON ghbin_sampledb.* TO 'cdc_writer'@'%';
