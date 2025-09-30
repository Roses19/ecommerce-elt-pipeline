WITH customer_orders AS (
    SELECT 
        dc.customer_unique_id,
        fo.order_id,
        dd.full_date AS order_date
    FROM olist_dw.fact_orders fo
    JOIN olist_dw.dim_customer dc 
        ON fo.customer_sk = dc.customer_sk
    JOIN olist_dw.dim_date dd 
        ON fo.order_purchase_date = dd.date_sk
),
firsts AS (
    SELECT customer_unique_id, MIN(order_date) AS first_order_date
    FROM customer_orders
    GROUP BY customer_unique_id
),
labeled AS (
    SELECT 
        co.customer_unique_id,
        co.order_id,
        co.order_date,
        CASE 
            WHEN co.order_date = f.first_order_date THEN 'New'
            ELSE 'Returning'
        END AS customer_type
    FROM customer_orders co
    JOIN firsts f USING (customer_unique_id)
)
SELECT 
    customer_type,
    COUNT(DISTINCT customer_unique_id) AS num_customers,
    COUNT(order_id) AS num_orders
FROM labeled
GROUP BY customer_type
ORDER BY customer_type;
