PRAGMA foreign_keys = ON;

-- Dimensions

DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT
);

INSERT INTO dim_customers VALUES
(1, 'Alice Smith', 'alice@example.com', 'USA'),
(2, 'Bob Johnson', 'bob@example.com', 'Canada'),
(3, 'Carol White', 'carol@example.com', 'UK');

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    price REAL
);

INSERT INTO dim_products VALUES
(1, 'Widget A', 'Widgets', 10.00),
(2, 'Widget B', 'Widgets', 15.00),
(3, 'Gadget X', 'Gadgets', 20.00);

DROP TABLE IF EXISTS dim_dates;
CREATE TABLE dim_dates (
    date_id INTEGER PRIMARY KEY,
    date TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday TEXT
);

INSERT INTO dim_dates VALUES
(20240601, '2024-06-01', 2024, 6, 1, 'Saturday'),
(20240602, '2024-06-02', 2024, 6, 2, 'Sunday'),
(20240603, '2024-06-03', 2024, 6, 3, 'Monday');

-- Fact table: orders

DROP TABLE IF EXISTS fact_orders;
CREATE TABLE fact_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    order_date_id INTEGER,
    quantity INTEGER,
    unit_price REAL,
    discount REAL,
    tax_amount REAL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (order_date_id) REFERENCES dim_dates(date_id)
);

INSERT INTO fact_orders VALUES
(1, 1, 1, 20240601, 2, 10.00, 0.1, 1.50),
(2, 2, 2, 20240602, 1, 15.00, 0.0, 1.20),
(3, 3, 3, 20240603, 3, 20.00, 0.05, 3.60);

-- Fact table: customer support

DROP TABLE IF EXISTS fact_customer_support;
CREATE TABLE fact_customer_support (
    support_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    date_id INTEGER,
    agent_name TEXT,
    resolution_time REAL,
    satisfaction_score INTEGER,
    issue_category TEXT,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
);

INSERT INTO fact_customer_support VALUES
(1, 1, 20240601, 'Agent A', 4.5, 5, 'Shipping'),
(2, 2, 20240602, 'Agent B', 2.0, 4, 'Technical'),
(3, 3, 20240603, 'Agent A', 6.0, 3, 'Billing');

-- Orders fact metrics

DROP VIEW IF EXISTS total_revenue;
CREATE VIEW total_revenue AS
SELECT SUM(quantity * unit_price * (1 - discount) + tax_amount) AS total_revenue
FROM fact_orders;

DROP VIEW IF EXISTS total_units_sold;
CREATE VIEW total_units_sold AS
SELECT SUM(quantity) AS total_units_sold
FROM fact_orders;

DROP VIEW IF EXISTS avg_order_value;
CREATE VIEW avg_order_value AS
SELECT SUM(quantity * unit_price * (1 - discount) + tax_amount) / COUNT(DISTINCT order_id) AS avg_order_value
FROM fact_orders;

DROP VIEW IF EXISTS total_discount;
CREATE VIEW total_discount AS
SELECT SUM(quantity * unit_price * discount) AS total_discount
FROM fact_orders;

-- Customer support fact metrics

DROP VIEW IF EXISTS total_tickets;
CREATE VIEW total_tickets AS
SELECT COUNT(*) AS total_tickets
FROM fact_customer_support;

DROP VIEW IF EXISTS avg_resolution_time;
CREATE VIEW avg_resolution_time AS
SELECT AVG(resolution_time) AS avg_resolution_time
FROM fact_customer_support;

DROP VIEW IF EXISTS avg_satisfaction_score;
CREATE VIEW avg_satisfaction_score AS
SELECT AVG(satisfaction_score) AS avg_satisfaction_score
FROM fact_customer_support;

