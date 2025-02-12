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
