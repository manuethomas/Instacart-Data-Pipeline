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
