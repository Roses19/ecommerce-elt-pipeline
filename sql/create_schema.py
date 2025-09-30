from sqlalchemy import create_engine, text
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import PG_URL, PG_SCHEMA

engine = create_engine(PG_URL, echo=True)

ddl = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

-- Dimension Tables

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    customer_state_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.dim_seller (
    seller_sk BIGSERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

DROP TABLE IF EXISTS {PG_SCHEMA}.dim_product CASCADE;

CREATE TABLE {PG_SCHEMA}.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE,
    product_category_name VARCHAR(100),
    product_name_length BIGINT,
    product_description_length BIGINT,
    product_photos_qty BIGINT,
    product_weight_g NUMERIC,
    product_length_cm NUMERIC,
    product_height_cm NUMERIC,
    product_width_cm NUMERIC,
    size_anomaly BOOLEAN,
    product_volume_cm3 BIGINT
);

DROP TABLE IF EXISTS {PG_SCHEMA}.dim_geolocation CASCADE;

CREATE TABLE {PG_SCHEMA}.dim_geolocation_clean AS
SELECT DISTINCT ON (geolocation_zip_code_prefix)
       geolocation_zip_code_prefix,
       geolocation_city,
       geolocation_state,
       latitude,
       longitude
FROM raw_geolocation
ORDER BY geolocation_zip_code_prefix;

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.dim_date (
    date_sk BIGSERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday VARCHAR(15)
);

-- Fact Tables

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_sk BIGINT REFERENCES {PG_SCHEMA}.dim_customer(customer_sk),
    order_status VARCHAR(20),
    order_purchase_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk),
    order_approved_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk),
    order_delivered_carrier_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk),
    order_delivered_customer_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk),
    order_estimated_delivery_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk)
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.fact_order_items (
    order_item_sk BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES {PG_SCHEMA}.fact_orders(order_id),
    product_sk BIGINT REFERENCES {PG_SCHEMA}.dim_product(product_sk),
    seller_sk BIGINT REFERENCES {PG_SCHEMA}.dim_seller(seller_sk),
    shipping_limit_date_sk BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk),
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.fact_payments (
    payment_sk BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES {PG_SCHEMA}.fact_orders(order_id),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.fact_reviews (
    review_sk BIGSERIAL PRIMARY KEY,
    review_id VARCHAR(50) UNIQUE,
    order_id VARCHAR(50) REFERENCES {PG_SCHEMA}.fact_orders(order_id),
    review_score INT CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date BIGINT REFERENCES {PG_SCHEMA}.dim_date(date_sk)
);
"""

if __name__ == "__main__":
    with engine.begin() as conn:
        conn.execute(text(ddl))
        res = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name=:s"),
            {"s": PG_SCHEMA}
        )
        print("Schemas tồn tại:", list(res))
