-- Sample queries for PostgreSQL Statistics Benchmarking
-- These queries test different scenarios and query patterns

-- Query 1: Simple aggregation with GROUP BY
SELECT country, COUNT(*) as customer_count, AVG(id) as avg_id
FROM customers 
WHERE created_at > '2023-01-01'
GROUP BY country 
ORDER BY customer_count DESC;

-- Query 2: JOIN with filtering
SELECT c.name, c.email, COUNT(o.id) as order_count, SUM(o.total_amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.country IN ('USA', 'UK', 'Canada')
AND o.status = 'completed'
GROUP BY c.id, c.name, c.email
HAVING SUM(o.total_amount) > 100
ORDER BY total_spent DESC
LIMIT 50;

-- Query 3: Complex JOIN with subquery
SELECT p.name, p.category, p.price,
       (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.id) as times_ordered
FROM products p
WHERE p.price BETWEEN 50 AND 500
AND p.category IN ('Electronics', 'Sports')
ORDER BY times_ordered DESC, p.price ASC;

-- Query 4: Date range filtering with aggregation
SELECT DATE(order_date) as order_day, 
       COUNT(*) as order_count,
       SUM(total_amount) as daily_revenue,
       AVG(total_amount) as avg_order_value
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
AND status IN ('completed', 'shipped')
GROUP BY DATE(order_date)
ORDER BY order_day DESC;

-- Query 5: Multiple table JOIN with complex conditions
SELECT c.city, 
       COUNT(DISTINCT o.id) as total_orders,
       COUNT(DISTINCT oi.product_id) as unique_products,
       SUM(oi.quantity * oi.unit_price) as total_revenue
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
WHERE o.order_date >= '2023-01-01'
AND p.category = 'Electronics'
AND o.status = 'completed'
GROUP BY c.city
HAVING COUNT(DISTINCT o.id) > 5
ORDER BY total_revenue DESC; 