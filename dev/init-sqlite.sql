-- Test database schema for SQLite
-- Simple e-commerce style data (same as MySQL/Postgres)

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    total REAL NOT NULL,
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert test data
INSERT INTO customers (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Carol White', 'carol@example.com'),
    ('David Brown', 'david@example.com'),
    ('Eve Davis', 'eve@example.com');

INSERT INTO products (name, price, stock) VALUES
    ('Laptop', 999.99, 50),
    ('Mouse', 29.99, 200),
    ('Keyboard', 79.99, 150),
    ('Monitor', 299.99, 75),
    ('Headphones', 149.99, 100);

INSERT INTO orders (customer_id, product_id, quantity, total) VALUES
    (1, 1, 1, 999.99),
    (1, 2, 2, 59.98),
    (2, 3, 1, 79.99),
    (3, 4, 2, 599.98),
    (4, 5, 1, 149.99),
    (5, 1, 1, 999.99),
    (2, 2, 3, 89.97),
    (3, 3, 1, 79.99);
