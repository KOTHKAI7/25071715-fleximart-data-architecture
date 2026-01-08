-- Query 1: Customer Purchase History
-- Business Question: Generate a detailed report showing each customer's name, email, total number of orders placed, and total amount spent.
-- Include only customers who have placed at least 2 orders and spent more than ₹5,000. Order by total amount spent desc.

SELECT
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.subtotal) AS total_spent
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY c.customer_id
HAVING COUNT(DISTINCT o.order_id) >= 2
   AND SUM(oi.subtotal) > 5000
ORDER BY total_spent DESC;


-- Query 2: Product Sales Analysis
-- Business Question: For each product category, show the category name, number of different products sold,
-- total quantity sold, and total revenue generated. Only include categories that have generated more than ₹10,000 in revenue.

SELECT
    p.category AS category,
    COUNT(DISTINCT p.product_id) AS num_products,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.subtotal) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.category
HAVING SUM(oi.subtotal) > 10000
ORDER BY total_revenue DESC;


-- Query 3: Monthly Sales Trend (Year 2024) 

WITH monthly AS (
  SELECT
    YEAR(o.order_date) AS order_year,
    MONTH(o.order_date) AS month_num,
    DATE_FORMAT(o.order_date, '%M') AS month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.subtotal) AS monthly_revenue
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  WHERE YEAR(o.order_date) = 2024
  GROUP BY
    YEAR(o.order_date),
    MONTH(o.order_date),
    DATE_FORMAT(o.order_date, '%M')
)
SELECT
  month_name,
  total_orders,
  monthly_revenue,
  SUM(monthly_revenue) OVER (
    ORDER BY month_num
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_revenue
FROM monthly
ORDER BY month_num;
