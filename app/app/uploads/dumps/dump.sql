-- Sample database dump for PostgreSQL Statistics Benchmarking
-- This creates a simple e-commerce-like schema for testing

-- Create tables
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);

-- Insert sample data
INSERT INTO customers (name, email, city, country) VALUES
('John Doe', 'john@example.com', 'New York', 'USA'),
('Jane Smith', 'jane@example.com', 'London', 'UK'),
('Mike Johnson', 'mike@example.com', 'Toronto', 'Canada'),
('Sarah Wilson', 'sarah@example.com', 'Sydney', 'Australia'),
('David Brown', 'david@example.com', 'Berlin', 'Germany');

-- Insert more customers with a generate_series approach
INSERT INTO customers (name, email, city, country)
SELECT 
    'Customer ' || i,
    'customer' || i || '@example.com',
    CASE (i % 5) 
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'London'
        WHEN 2 THEN 'Toronto'
        WHEN 3 THEN 'Sydney'
        ELSE 'Berlin'
    END,
    CASE (i % 5)
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'Canada'
        WHEN 3 THEN 'Australia'
        ELSE 'Germany'
    END
FROM generate_series(6, 1000) i;

INSERT INTO products (name, category, price, stock_quantity) VALUES
('Laptop Pro', 'Electronics', 1299.99, 50),
('Wireless Mouse', 'Electronics', 29.99, 200),
('Office Chair', 'Furniture', 199.99, 75),
('Coffee Mug', 'Kitchen', 12.99, 300),
('Running Shoes', 'Sports', 89.99, 120);

-- Insert more products
INSERT INTO products (name, category, price, stock_quantity)
SELECT 
    'Product ' || i,
    CASE (i % 4)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Furniture'
        WHEN 2 THEN 'Kitchen'
        ELSE 'Sports'
    END,
    (random() * 1000 + 10)::DECIMAL(10,2),
    (random() * 200 + 10)::INTEGER
FROM generate_series(6, 500) i;

-- Insert orders
INSERT INTO orders (customer_id, total_amount, status, order_date)
SELECT 
    (random() * 999 + 1)::INTEGER,
    (random() * 500 + 20)::DECIMAL(10,2),
    CASE (random() * 3)::INTEGER
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        ELSE 'shipped'
    END,
    CURRENT_TIMESTAMP - (random() * interval '365 days')
FROM generate_series(1, 5000);

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT 
    o.id,
    (random() * 499 + 1)::INTEGER,
    (random() * 5 + 1)::INTEGER,
    p.price
FROM orders o
CROSS JOIN LATERAL (
    SELECT price FROM products 
    WHERE id = (random() * 499 + 1)::INTEGER 
    LIMIT 1
) p
WHERE random() < 0.3;  -- Each order has about 30% chance of having items

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- Analyze tables to gather statistics
ANALYZE; 