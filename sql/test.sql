SET search_path TO olist_dw;
SELECT * FROM olist_dw.dim_customer LIMIT 10;
SELECT * FROM olist_dw.dim_date LIMIT 10;
SELECT * FROM olist_dw.dim_geolocation LIMIT 10;
SELECT * FROM olist_dw.dim_seller LIMIT 10;
SELECT * FROM olist_dw.dim_product LIMIT 10;
SELECT * FROM olist_dw.fact_orders LIMIT 10;
SELECT * FROM olist_dw.fact_order_items LIMIT 10;
SELECT * FROM olist_dw.fact_payments LIMIT 10;