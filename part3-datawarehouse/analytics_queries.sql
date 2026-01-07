USE fleximart_dw;

-- 1. Total sales by product category
SELECT
    dp.category,
    SUM(fs.total_amount) AS total_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.category;

-- 2. Monthly sales trend
SELECT
    dd.year,
    dd.month_name,
    SUM(fs.total_amount) AS monthly_sales
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- 3. Top customers by total spending
SELECT
    dc.customer_name,
    SUM(fs.total_amount) AS total_spent
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_name
ORDER BY total_spent DESC;

-- 4. Average order value per customer
SELECT
    dc.customer_name,
    AVG(fs.total_amount) AS avg_order_value
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_name;
