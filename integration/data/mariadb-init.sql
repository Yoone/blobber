-- Test database schema for MariaDB
-- Simple e-commerce style data

CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert test data
INSERT INTO customers (name, email) VALUES
    ('Frank Miller', 'frank@example.com'),
    ('Grace Lee', 'grace@example.com'),
    ('Henry Wilson', 'henry@example.com'),
    ('Ivy Taylor', 'ivy@example.com'),
    ('Jack Anderson', 'jack@example.com');

INSERT INTO products (name, price, stock) VALUES
    ('Tablet', 499.99, 80),
    ('Charger', 19.99, 300),
    ('Case', 39.99, 250),
    ('Stand', 49.99, 120),
    ('Cable', 9.99, 500);

INSERT INTO orders (customer_id, product_id, quantity, total) VALUES
    (1, 1, 1, 499.99),
    (1, 2, 2, 39.98),
    (2, 3, 1, 39.99),
    (3, 4, 2, 99.98),
    (4, 5, 5, 49.95),
    (5, 1, 1, 499.99),
    (2, 2, 3, 59.97),
    (3, 3, 2, 79.98);
