-- Create Database
-- CREATE DATABASE insta_cart_db

-- Create tables
DROP TABLE IF EXISTS fact_orders;
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order INT,
    eval_set VARCHAR(255)
);

DROP TABLE IF EXISTS dim_departments;
CREATE TABLE dim_departments (
    department_id INT PRIMARY KEY,
    department VARCHAR(255)
);

DROP TABLE IF EXISTS dim_aisles;
CREATE TABLE dim_aisles (
    aisle_id INT PRIMARY KEY,
    aisle VARCHAR(255)
);

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    aisle_id INT,
    department_id INT,
    FOREIGN KEY (department_id) REFERENCES dim_departments(department_id),
    FOREIGN KEY (aisle_id) REFERENCES dim_aisles(aisle_id)
);

DROP TABLE IF EXISTS fact_order_products;
CREATE TABLE fact_order_products (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

DROP TABLE IF EXISTS fact_order_products_prior;
CREATE TABLE fact_order_products_prior (
    order_id INT,
    product_id INT,
    add_to_cart_order INT,
    reordered INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);
