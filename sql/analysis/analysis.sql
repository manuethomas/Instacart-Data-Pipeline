-- SQL Analysis--


/*Peek at the dataset*/
-----------------------

SELECT *
FROM
    fact_orders
LIMIT 10

SELECT *
FROM
    fact_order_products
LIMIT 10

SELECT *
FROM
    fact_order_products_prior
LIMIT 10

SELECT *
FROM
    dim_aisles
LIMIT 10

SELECT *
FROM
    dim_departments
LIMIT 10

SELECT *
FROM
    dim_products
LIMIT 10


/*1. Order behaviour based on hour of day, day of week. */
----------------------------------------------------------

-- Count of orders by hour of day
SELECT 
    order_hour_of_day,
    COUNT(*) AS order_count
FROM 
    fact_orders
GROUP BY 
    order_hour_of_day
ORDER BY
    order_hour_of_day ;

-- Count of orders by day of week
SELECT 
    order_dow AS day_of_week,
    COUNT(*)
FROM
    fact_orders
GROUP BY
    order_dow
ORDER BY
    order_dow;


/*2. What is the order volume by the days since prior order ?
-------------------------------------------------------------
  
  */
SELECT
    days_since_prior_order,
    COUNT(*) AS order_count
FROM
    fact_orders
GROUP BY
    days_since_prior_order
ORDER BY
    days_since_prior_order;


/*3. What is the order volume by prior order ?*/
------------------------------------------------
SELECT
    order_number AS prior_order,
    COUNT(*) AS order_count
FROM
    fact_orders as orders
WHERE
    eval_set = 'prior'
GROUP BY
    order_number
ORDER BY
    order_number;


/*4. What is the basket size for each order?*/
----------------------------------------------
SELECT
    order_id,
    MAX(add_to_cart_order) AS basket_size
FROM
    fact_order_products
GROUP BY
    order_id
ORDER BY
    basket_size DESC;

SELECT
    order_id,
    MAX(add_to_cart_order) AS basket_size
FROM
    fact_order_products_prior
GROUP BY
    order_id
ORDER BY
    basket_size DESC;


/*5. What are the top 10 products sold ?*/
------------------------------------------
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
LIMIT 10;


/*6. What is the proportion of reordered orders ?*/
---------------------------------------------------
SELECT
    reordered,
    COUNT(*) / SUM(COUNT(*)) OVER () AS proportion
FROM
    fact_order_products
GROUP BY
    reordered;


/*7. What are the most frequently reordered products ?*/
--------------------------------------------------------
SELECT
    p.product_id,
    p.product_name,
    COUNT(*) AS reorder_count
FROM
    fact_order_products AS orders
JOIN dim_products AS p ON p.product_id = orders.product_id
WHERE
    reordered = 1
GROUP BY
    p.product_id
ORDER BY
    reorder_count DESC
LIMIT 10;


/*8. What are the top 10 reordered products based on the reorder proportion having a minimum order count of 40 ?*/
------------------------------------------------------------------------------------------------------------------

WITH reorder_proportion AS (
    SELECT
        product_id,
        AVG(reordered) AS proportion_reordered, -- proportion of reordered (1's)
        COUNT(*) AS order_count
    FROM
        fact_order_products AS orders
    GROUP BY
        product_id
    HAVING
        COUNT(*) > 40 -- weed out single orders
)
SELECT
    p.product_id,
    p.product_name,
    rp.proportion_reordered,
    rp.order_count
FROM
    reorder_proportion AS rp
JOIN dim_products AS p ON p.product_id = rp.product_id
ORDER BY
    rp.proportion_reordered DESC
LIMIT 10;


/*9. Which are the top 10 items added to cart first ?*/
-------------------------------------------------------
WITH first_item AS (
    SELECT
        product_id,
        COUNT(*) AS order_count
    FROM
        fact_order_products AS orders
    WHERE
        add_to_cart_order = 1
    GROUP BY
    product_id
    HAVING
        COUNT(*) > 1
)
SELECT
    fi.product_id,
    p.product_name,
    fi.order_count AS first_time_count
FROM
    first_item as fi
JOIN dim_products AS p ON p.product_id = fi.product_id
ORDER BY
    fi.order_count DESC
LIMIT 10;


/*10. Association between days since last order and probability of reorder*/
----------------------------------------------------------------------------
SELECT
    tot_orders.days_since_prior_order,
    AVG(orders.reordered) AS prob_reordered
FROM
    fact_order_products AS orders
JOIN fact_orders AS tot_orders ON tot_orders.order_id = orders.order_id
GROUP BY
    tot_orders.days_since_prior_order
ORDER BY
    AVG(orders.reordered) DESC;


/*11. Association between number of orders and probability of reordering*/
--------------------------------------------------------------------------
WITH reorder_proportion AS (
    SELECT
        product_id,
        AVG(reordered) AS proportion_reordered, -- proportion of reordered (1's)
        COUNT(*) AS order_count
    FROM
        fact_order_products AS orders
    GROUP BY
        product_id
    HAVING
        COUNT(*) > 1 -- weed out single orders
)
SELECT
    p.product_id,
    p.product_name,
    rp.proportion_reordered,
    rp.order_count
FROM
    reorder_proportion AS rp
JOIN dim_products AS p ON p.product_id = rp.product_id
ORDER BY
    order_count DESC
LIMIT 10;


/*12. Proportion of organic v/s non-organic orders*/
----------------------------------------------------
WITH organic_products AS (
    SELECT
        product_id,
        product_name,
        CASE
            WHEN LOWER(product_name) LIKE '%organic%' THEN 'organic' 
            ELSE 'non organic'
        END AS organic
    FROM
        dim_products
)
SELECT
    o.organic,
    COUNT(*) / SUM(COUNT(*)) OVER () AS proportion  -- OVER() -> window contains the entire result set of group by
FROM
    fact_order_products AS orders
JOIN organic_products AS o ON o.product_id = orders.product_id
GROUP BY
    o.organic;


/*13. Proportion of organic v/s non organic reorders*/
------------------------------------------------------
WITH organic_products AS (
    SELECT
        product_id,
        product_name,
        CASE
            WHEN LOWER(product_name) LIKE '%organic%' THEN 'organic' 
            ELSE 'non organic'
        END AS organic
    FROM
        dim_products
)
SELECT
    o.organic,
    COUNT(*) / SUM(COUNT(*)) OVER () AS proportion  -- OVER() -> window contains the entire result set of group by
FROM
    fact_order_products AS orders
JOIN organic_products AS o ON o.product_id = orders.product_id
WHERE 
    orders.reordered = 1
GROUP BY
    o.organic;












