-- Conduit demo database: sample e-commerce data
-- This SQL is embedded and run against an in-memory SQLite database.

CREATE TABLE customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'US',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    sku TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL DEFAULT 'pending',
    total REAL NOT NULL,
    shipping_address TEXT,
    ordered_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL
);

CREATE TABLE reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL REFERENCES products(id),
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title TEXT,
    body TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Seed data: customers
INSERT INTO customers (first_name, last_name, email, phone, city, state) VALUES
    ('Alice', 'Johnson', 'alice@example.com', '555-0101', 'San Francisco', 'CA'),
    ('Bob', 'Smith', 'bob@example.com', '555-0102', 'New York', 'NY'),
    ('Carol', 'Williams', 'carol@example.com', '555-0103', 'Austin', 'TX'),
    ('David', 'Brown', 'david@example.com', '555-0104', 'Seattle', 'WA'),
    ('Eve', 'Davis', 'eve@example.com', '555-0105', 'Portland', 'OR'),
    ('Frank', 'Miller', 'frank@example.com', '555-0106', 'Denver', 'CO'),
    ('Grace', 'Wilson', 'grace@example.com', '555-0107', 'Chicago', 'IL'),
    ('Henry', 'Taylor', 'henry@example.com', '555-0108', 'Boston', 'MA');

-- Seed data: products
INSERT INTO products (name, category, price, stock, sku, description) VALUES
    ('Mechanical Keyboard', 'Electronics', 149.99, 45, 'KB-MK-001', 'Cherry MX Brown switches, RGB backlit'),
    ('Wireless Mouse', 'Electronics', 49.99, 120, 'MS-WL-002', 'Ergonomic design, 4000 DPI'),
    ('USB-C Hub', 'Electronics', 39.99, 80, 'HB-UC-003', '7-in-1 hub with HDMI, SD card, USB-A'),
    ('Standing Desk Mat', 'Office', 59.99, 35, 'MT-SD-004', 'Anti-fatigue mat, 20x36 inches'),
    ('Laptop Stand', 'Office', 34.99, 65, 'ST-LP-005', 'Adjustable aluminum stand'),
    ('Monitor Light Bar', 'Electronics', 44.99, 55, 'LT-MB-006', 'LED light bar, auto-dimming'),
    ('Webcam HD', 'Electronics', 79.99, 40, 'WC-HD-007', '1080p with built-in mic'),
    ('Desk Organizer', 'Office', 24.99, 90, 'OR-DK-008', 'Bamboo desktop organizer'),
    ('Noise Canceling Headphones', 'Audio', 199.99, 25, 'HP-NC-009', 'ANC, 30hr battery, Bluetooth 5.3'),
    ('Portable Charger', 'Electronics', 29.99, 150, 'CH-PT-010', '10000mAh, USB-C PD 20W');

-- Seed data: orders
INSERT INTO orders (customer_id, status, total, shipping_address, ordered_at) VALUES
    (1, 'delivered', 199.98, '123 Market St, San Francisco, CA', '2025-01-15 10:30:00'),
    (2, 'delivered', 49.99, '456 Broadway, New York, NY', '2025-01-18 14:22:00'),
    (3, 'shipped', 234.97, '789 Congress Ave, Austin, TX', '2025-02-01 09:15:00'),
    (1, 'delivered', 79.99, '123 Market St, San Francisco, CA', '2025-02-05 16:45:00'),
    (4, 'processing', 149.99, '321 Pine St, Seattle, WA', '2025-02-20 11:00:00'),
    (5, 'pending', 329.97, '654 Oak Ave, Portland, OR', '2025-02-22 08:30:00'),
    (6, 'delivered', 59.99, '987 Elm St, Denver, CO', '2025-01-28 13:10:00'),
    (7, 'shipped', 104.98, '147 Lake Dr, Chicago, IL', '2025-02-18 15:55:00'),
    (2, 'delivered', 199.99, '456 Broadway, New York, NY', '2025-02-10 10:00:00'),
    (8, 'processing', 74.98, '258 Harbor Rd, Boston, MA', '2025-02-23 09:45:00');

-- Seed data: order_items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 149.99), (1, 2, 1, 49.99),
    (2, 2, 1, 49.99),
    (3, 1, 1, 149.99), (3, 5, 1, 34.99), (3, 2, 1, 49.99),
    (4, 7, 1, 79.99),
    (5, 1, 1, 149.99),
    (6, 9, 1, 199.99), (6, 10, 1, 29.99), (6, 6, 1, 44.99), (6, 8, 1, 24.99),
    (7, 4, 1, 59.99),
    (8, 5, 1, 34.99), (8, 3, 1, 39.99), (8, 10, 1, 29.99),
    (9, 9, 1, 199.99),
    (10, 5, 1, 34.99), (10, 3, 1, 39.99);

-- Seed data: reviews
INSERT INTO reviews (product_id, customer_id, rating, title, body, created_at) VALUES
    (1, 1, 5, 'Best keyboard ever', 'The tactile feedback is amazing. Worth every penny.', '2025-01-20 12:00:00'),
    (1, 3, 4, 'Great but loud', 'Excellent build quality. A bit louder than expected.', '2025-02-05 09:30:00'),
    (2, 2, 5, 'Perfect mouse', 'Fits my hand perfectly. Battery lasts weeks.', '2025-01-22 14:15:00'),
    (9, 6, 5, 'Incredible ANC', 'The noise cancellation is on another level.', '2025-02-01 16:00:00'),
    (9, 2, 4, 'Great sound', 'Audio quality is excellent. ANC could be stronger.', '2025-02-12 11:30:00'),
    (5, 4, 3, 'Does the job', 'Simple but effective. Wish it was more adjustable.', '2025-02-22 08:45:00'),
    (7, 1, 4, 'Good webcam', 'Sharp image. Auto-focus works well in most conditions.', '2025-02-08 10:20:00'),
    (3, 8, 5, 'Essential accessory', 'Every laptop user needs this. All ports you need.', '2025-02-24 13:00:00');

-- Create useful indexes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_reviews_product ON reviews(product_id);
CREATE INDEX idx_products_category ON products(category);
