/*-------------------------------------Create Tables-----------------------------------------------*/

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



/*----------------------------------Stored Procedures-----------------------------------------------*/



/*----------------------------------------Views-----------------------------------------------------*/

-- Views --

/*Create a view for organic v/s non organic products*/
-----------------------------------------------------
CREATE VIEW OrganicProducts AS (
    SELECT
        product_id,
        product_name,
        CASE
            WHEN LOWER(product_name) LIKE '%organic%' THEN 'organic'
            ELSE 'not organic'
        END AS organic
    FROM
        dim_products
);


/*Create a view for reorder proportion by product*/
---------------------------------------------------
CREATE VIEW ReorderedProportion AS (
    WITH reorder_proportion AS (
        SELECT
            orders.product_id,
            AVG(orders.reordered) AS reordered_proportion,
            COUNT(*) AS order_count
        FROM
            fact_order_products AS orders
        GROUP BY
            orders.product_id
    )
    SELECT
        p.product_id,
        p.product_name,
        rp.reordered_proportion,
        rp.order_count
    FROM
        reorder_proportion AS rp
    JOIN dim_products AS p ON p.product_id = rp.product_id
    ORDER BY
        order_count DESC
    LIMIT 10
);

/*Create a view for customers who always reorder*/
--------------------------------------------------
CREATE VIEW AlwaysReorderCustomers AS (
    WITH reorder_proportion AS (
        SELECT 
            order_id,
            COUNT(*) AS order_count,
            AVG(reordered) AS reorder_proportion
        FROM
            fact_order_products_prior AS prior_orders
        GROUP BY
            order_id
    )
    SELECT
        orders.user_id,
        COUNT(*) AS order_count
    FROM
        fact_orders AS orders
    JOIN reorder_proportion AS rp ON rp.order_id = orders.order_id
    WHERE
        orders.order_number > 2 AND orders.eval_set = 'prior'
    GROUP BY
        orders.user_id
    HAVING
        COUNT(*) = SUM(CASE 
                    WHEN rp.reorder_proportion = 1 THEN 1 
                    ELSE 0 
                    END) -- only users having reorder_proportion = 1 for all orders
);


/*Create a view to see the top 10 products sold*/
CREATE VIEW Top10Products AS (
    SELECT
        p.product_name AS product_name,
        COUNT(*) AS order_count
    FROM
        fact_order_products AS orders
    JOIN dim_products AS p on p.product_id = orders.product_id
    GROUP BY
        p.product_name
    ORDER BY
        order_count DESC
    LIMIT 10
);



/*-------------------------------------Triggers-----------------------------------------------*/

/*Create a trigger to maintain an audit log whenever a new order is inserted into the fact_orders table*/
---------------------------------------------------------------------------------------------------------

-- Create an audit log table
CREATE TABLE IF NOT EXISTS order_audit_log (
    audit_id SERIAL PRIMARY KEY,
    order_id INT,
    user_id INT,
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order INT,
    audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alter table to set foreign key
ALTER TABLE order_audit_log
ADD CONSTRAINT fk_order_audit_log
FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);

-- Trigger function to insert a record into audit log table
CREATE OR REPLACE FUNCTION log_order_insert()
RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO order_audit_log(order_id, user_id, order_number, order_dow, order_hour_of_day, days_since_prior_order)
    VALUES(NEW.order_id, NEW.user_id, NEW.order_number, NEW.order_dow, NEW.order_hour_of_day, NEW.days_since_prior_order);
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- Trigger to call the function after a new order is inserted
CREATE TRIGGER after_order_insert
AFTER INSERT ON fact_orders
FOR EACH ROW
EXECUTE FUNCTION log_order_insert();


/*Create a trigger to ensure that the add_to_cart_order is always a positive integer*/
--------------------------------------------------------------------------------------

-- Trigger function to enforce add_to_cart_order rule
CREATE OR REPLACE FUNCTION enforce_order_product_rules()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if add_to_cart_order is a positive integer
    IF NEW.add_to_cart_order <= 0 THEN
        RAISE EXCEPTION 'add_to_cart_order must be a positive integer';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the function before a new order product is inserted
CREATE TRIGGER before_order_product_insert
BEFORE INSERT ON fact_order_products
FOR EACH ROW
EXECUTE FUNCTION enforce_order_product_rules();
