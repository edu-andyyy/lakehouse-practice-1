-- Each staging view assigns one reason to an invalid row. Bronze is preserved.
CREATE OR REPLACE VIEW lakehouse.silver.customer_stage AS
SELECT *, CASE
    WHEN custkey IS NULL OR custkey <= 0 THEN 'invalid_customer_key'
    WHEN name IS NULL OR trim(name) = '' THEN 'empty_customer_name'
    WHEN mktsegment IS NULL OR trim(mktsegment) = '' THEN 'empty_segment'
END AS rejection_reason
FROM lakehouse.bronze.customer;

CREATE OR REPLACE VIEW lakehouse.silver.orders_stage AS
SELECT o.*, CASE
    WHEN orderkey IS NULL OR orderkey <= 0 THEN 'invalid_order_key'
    WHEN orderdate IS NULL THEN 'missing_order_date'
    WHEN shippriority IS NULL OR shippriority < 0 THEN 'invalid_ship_priority'
    WHEN c.custkey IS NULL THEN 'unknown_customer'
END AS rejection_reason
FROM lakehouse.bronze.orders o
LEFT JOIN lakehouse.silver.customer_stage c
    ON o.custkey = c.custkey AND c.rejection_reason IS NULL;

CREATE OR REPLACE VIEW lakehouse.silver.lineitem_stage AS
SELECT l.*, CASE
    WHEN l.orderkey IS NULL OR l.orderkey <= 0 THEN 'invalid_order_key'
    WHEN linenumber IS NULL OR linenumber < 1 THEN 'invalid_line_number'
    WHEN quantity IS NULL OR quantity <= 0
        OR try_cast(quantity AS DECIMAL(12,2)) IS NULL THEN 'invalid_quantity'
    WHEN extendedprice IS NULL OR extendedprice < 0
        OR try_cast(extendedprice AS DECIMAL(15,2)) IS NULL THEN 'invalid_price'
    WHEN discount IS NULL OR discount < 0 OR discount > 1
        OR try_cast(discount AS DECIMAL(4,2)) IS NULL THEN 'invalid_discount'
    WHEN shipdate IS NULL THEN 'missing_ship_date'
    WHEN o.orderkey IS NULL THEN 'unknown_order'
END AS rejection_reason
FROM lakehouse.bronze.lineitem l
LEFT JOIN lakehouse.silver.orders_stage o
    ON l.orderkey = o.orderkey AND o.rejection_reason IS NULL;

CREATE TABLE lakehouse.silver.rejected_records
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT 'customer' AS source_table, cast(custkey AS varchar) AS record_key,
    rejection_reason FROM lakehouse.silver.customer_stage WHERE rejection_reason IS NOT NULL
UNION ALL
SELECT 'orders', cast(orderkey AS varchar), rejection_reason
FROM lakehouse.silver.orders_stage WHERE rejection_reason IS NOT NULL
UNION ALL
SELECT 'lineitem', concat(cast(orderkey AS varchar), ':', cast(linenumber AS varchar)),
    rejection_reason FROM lakehouse.silver.lineitem_stage WHERE rejection_reason IS NOT NULL;

CREATE TABLE lakehouse.silver.customer
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT custkey, trim(name) AS name, upper(trim(mktsegment)) AS mktsegment
FROM lakehouse.silver.customer_stage WHERE rejection_reason IS NULL;

CREATE TABLE lakehouse.silver.orders
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, custkey, orderdate, shippriority
FROM lakehouse.silver.orders_stage WHERE rejection_reason IS NULL;

CREATE TABLE lakehouse.silver.lineitem
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, linenumber, cast(quantity AS DECIMAL(12,2)) AS quantity,
    cast(extendedprice AS DECIMAL(15,2)) AS extendedprice,
    cast(discount AS DECIMAL(4,2)) AS discount, shipdate
FROM lakehouse.silver.lineitem_stage WHERE rejection_reason IS NULL;

CREATE TABLE lakehouse.silver.shipping_lines
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT l.orderkey, l.linenumber, c.custkey, c.name AS customer_name,
    c.mktsegment, o.orderdate, o.shippriority, l.shipdate, l.quantity,
    l.extendedprice, l.discount,
    cast(l.extendedprice * (DECIMAL '1.00' - l.discount) AS DECIMAL(20,4)) AS net_revenue,
    year(o.orderdate) AS order_year,
    date_diff('day', o.orderdate, l.shipdate) AS days_to_ship
FROM lakehouse.silver.customer c
JOIN lakehouse.silver.orders o ON c.custkey = o.custkey
JOIN lakehouse.silver.lineitem l ON o.orderkey = l.orderkey
WHERE c.mktsegment = 'BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15';
