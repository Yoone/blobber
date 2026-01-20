-- Test database schema for PostgreSQL
-- Simple e-commerce style data

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    product_id INT NOT NULL REFERENCES products(id),
    quantity INT NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO customers (name, email) VALUES
    ('Kate Murphy', 'kate@example.com'),
    ('Leo Garcia', 'leo@example.com'),
    ('Mia Hert', 'mia@example.com'),
    ('Noah Martinez', 'noah@example.com'),
    ('Olivia Hernandez', 'olivia@example.com');

INSERT INTO products (name, price, stock) VALUES
    ('Camera', 799.99, 40),
    ('Lens', 399.99, 60),
    ('Tripod', 89.99, 100),
    ('Bag', 59.99, 150),
    ('Filter', 29.99, 200);

INSERT INTO orders (customer_id, product_id, quantity, total) VALUES
    (1, 1, 1, 799.99),
    (1, 2, 1, 399.99),
    (2, 3, 1, 89.99),
    (3, 4, 2, 119.98),
    (4, 5, 3, 89.97),
    (5, 1, 1, 799.99),
    (2, 2, 1, 399.99),
    (3, 3, 2, 179.98);
