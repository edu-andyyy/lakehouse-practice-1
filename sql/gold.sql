CREATE TABLE lakehouse.gold.shipping_priority
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, sum(net_revenue) AS revenue, orderdate, shippriority
FROM lakehouse.silver.shipping_lines
GROUP BY orderkey, orderdate, shippriority
ORDER BY revenue DESC
LIMIT 10
