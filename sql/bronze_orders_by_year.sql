SELECT year(orderdate) AS order_year, count(*) AS orders_count
FROM lakehouse.bronze.orders
GROUP BY 1
ORDER BY 1
