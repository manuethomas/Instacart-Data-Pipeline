
-- Create tables
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    eval_set VARCHAR(255),
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order INT
);

CREATE TABLE IF NOT EXISTS dim_departments (
    department_id INT PRIMARY KEY,
    department VARCHAR(255)
);

CREATE TABLE dim_aisles (
    aisle_id INT PRIMARY KEY,
    aisle VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    aisle_id INT,
    department_id INT,
    FOREIGN KEY (department_id) REFERENCES dim_departments(department_id),
    FOREIGN KEY (aisle_id) REFERENCES dim_aisles(aisle_id)
);

CREATE TABLE IF NOT EXISTS fact_order_products (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered INT,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

CREATE TABLE IF NOT EXISTS fact_order_products_prior (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered INT,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);
