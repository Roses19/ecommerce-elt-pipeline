import pandas as pd
from sqlalchemy import create_engine, text
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, PG_URL
import numpy as np
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS,
    "secret": MINIO_SECRET,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
}

engine = create_engine(PG_URL)
SCHEMA = "olist_dw"

def load_dim_date(conn):
    orders = pd.read_parquet(
        f"s3://{MINIO_BUCKET}/silver/orders.parquet",
        storage_options=STORAGE_OPTIONS
    )

    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    all_dates = pd.concat([orders[c].dropna() for c in date_cols])
    all_dates = all_dates.dt.normalize().drop_duplicates().sort_values()

    dim_date = pd.DataFrame({
        "full_date": all_dates,
        "year": all_dates.dt.year,
        "quarter": all_dates.dt.quarter,
        "month": all_dates.dt.month,
        "day": all_dates.dt.day,
        "weekday": all_dates.dt.day_name()
    })

    for _, row in dim_date.iterrows():
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.dim_date (full_date, year, quarter, month, day, weekday)
            VALUES (:full_date, :year, :quarter, :month, :day, :weekday)
            ON CONFLICT (full_date) DO NOTHING;
        """), row.to_dict())

    print(f"Da load dim_date: {len(dim_date)} unique days")


def load_dim_customer(conn):
    df = pd.read_parquet(f"s3://{MINIO_BUCKET}/silver/customers.parquet",
                         storage_options=STORAGE_OPTIONS)
    for _, row in df.iterrows():
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.dim_customer 
            (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, customer_state_name)
            VALUES (:customer_id, :customer_unique_id, :customer_zip_code_prefix, :customer_city, :customer_state, :customer_state_name)
            ON CONFLICT (customer_id) DO UPDATE 
            SET customer_city = EXCLUDED.customer_city,
                customer_state = EXCLUDED.customer_state,
                customer_state_name = EXCLUDED.customer_state_name;
        """), row.to_dict())
    print(f"Da load dim_customer: {len(df)} dong")

def load_dim_seller(conn):
    df = pd.read_parquet(f"s3://{MINIO_BUCKET}/silver/sellers.parquet",
                         storage_options=STORAGE_OPTIONS)
    for _, row in df.iterrows():
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.dim_seller 
            (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            VALUES (:seller_id, :seller_zip_code_prefix, :seller_city, :seller_state)
            ON CONFLICT (seller_id) DO UPDATE 
            SET seller_city = EXCLUDED.seller_city,
                seller_state = EXCLUDED.seller_state;
        """), row.to_dict())
    print(f"Da load dim_seller: {len(df)} dong")

def load_dim_product(conn):
    df = pd.read_parquet(
        f"s3://{MINIO_BUCKET}/silver/products.parquet",
        storage_options=STORAGE_OPTIONS
    )

    df["product_volume_cm3"] = (
        df["product_length_cm"] * df["product_height_cm"] * df["product_width_cm"]
    )


    df["size_anomaly"] = df["size_anomaly"].apply(
        lambda x: bool(x) if pd.notnull(x) else None
    )

    for col in ["product_name_length", "product_description_length", "product_photos_qty", "product_volume_cm3"]:
        df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

    df = df.replace({np.nan: None})

    insert_sql = text("""
        INSERT INTO olist_dw.dim_product (
            product_id, product_category_name, product_name_length, product_description_length,
            product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            size_anomaly, product_volume_cm3
        )
        VALUES (
            :product_id, :product_category_name, :product_name_length, :product_description_length,
            :product_photos_qty, :product_weight_g, :product_length_cm, :product_height_cm, :product_width_cm,
            :size_anomaly, :product_volume_cm3
        )
        ON CONFLICT (product_id) DO UPDATE 
        SET product_category_name = EXCLUDED.product_category_name,
            product_weight_g = EXCLUDED.product_weight_g,
            product_volume_cm3 = EXCLUDED.product_volume_cm3;
    """)

    for _, row in df.iterrows():
        conn.execute(insert_sql, row.to_dict())

    print(f"Da load dim_product: {df.shape[0]} dong")

def load_fact_orders(conn):
    path = f"s3://{MINIO_BUCKET}/silver/orders.parquet"
    df = pd.read_parquet(path, storage_options=STORAGE_OPTIONS)

    customer_df = pd.read_sql("SELECT customer_id, customer_sk FROM olist_dw.dim_customer", conn)
    df = df.merge(customer_df, on="customer_id", how="left")

    date_df = pd.read_sql("SELECT full_date, date_sk FROM olist_dw.dim_date", conn)
    date_df["full_date"] = pd.to_datetime(date_df["full_date"]).dt.normalize()

    date_cols = {
        "order_purchase_timestamp": "order_purchase_date_sk",
        "order_approved_at": "order_approved_date_sk",
        "order_delivered_carrier_date": "order_delivered_carrier_date_sk",
        "order_delivered_customer_date": "order_delivered_customer_date_sk",
        "order_estimated_delivery_date": "order_estimated_delivery_date_sk"
    }

    for src_col, sk_col in date_cols.items():
        df[src_col] = pd.to_datetime(df[src_col], errors="coerce").dt.normalize()
        df = df.merge(date_df, left_on=src_col, right_on="full_date", how="left")
        df = df.rename(columns={"date_sk": sk_col})
        df = df.drop(columns=["full_date"])

    fact_df = df[[
        "order_id", "customer_sk", "order_status",
        "order_purchase_date_sk", "order_approved_date_sk",
        "order_delivered_carrier_date_sk", "order_delivered_customer_date_sk",
        "order_estimated_delivery_date_sk"
    ]].replace({np.nan: None})

    for _, row in fact_df.iterrows():
        conn.execute(text("""
            INSERT INTO olist_dw.fact_orders
            (order_id, customer_sk, order_status,
             order_purchase_date, order_approved_date,
             order_delivered_carrier_date, order_delivered_customer_date,
             order_estimated_delivery_date)
            VALUES (:order_id, :customer_sk, :order_status,
                    :order_purchase_date_sk, :order_approved_date_sk,
                    :order_delivered_carrier_date_sk, :order_delivered_customer_date_sk,
                    :order_estimated_delivery_date_sk)
            ON CONFLICT DO NOTHING;
        """), row.to_dict())

    print(f"Da load fact_orders: {len(fact_df)} dong")


def load_fact_order_items(conn):
    path = f"s3://{MINIO_BUCKET}/silver/order_items.parquet"
    df = pd.read_parquet(path, storage_options=STORAGE_OPTIONS)


    product_df = pd.read_sql("SELECT product_id, product_sk FROM olist_dw.dim_product", conn)
    df = df.merge(product_df, on="product_id", how="left")

    seller_df = pd.read_sql("SELECT seller_id, seller_sk FROM olist_dw.dim_seller", conn)
    df = df.merge(seller_df, on="seller_id", how="left")

    date_df = pd.read_sql("SELECT full_date, date_sk FROM olist_dw.dim_date", conn)
    date_df["full_date"] = pd.to_datetime(date_df["full_date"]).dt.normalize()

    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce").dt.normalize()
    df = df.merge(date_df, left_on="shipping_limit_date", right_on="full_date", how="left")
    df = df.rename(columns={"date_sk": "shipping_limit_date_sk"}).drop(columns=["full_date"])

    fact_df = df[[
        "order_id", "product_sk", "seller_sk", "shipping_limit_date_sk",
        "price", "freight_value"
    ]].replace({np.nan: None})

    for _, row in fact_df.iterrows():
        conn.execute(text("""
            INSERT INTO olist_dw.fact_order_items
            (order_id, product_sk, seller_sk, shipping_limit_date_sk, price, freight_value)
            VALUES (:order_id, :product_sk, :seller_sk, :shipping_limit_date_sk, :price, :freight_value)
            ON CONFLICT DO NOTHING;
        """), row.to_dict())

    print(f"Da load fact_order_items: {len(fact_df)} dong")


def load_fact_payments(conn):
    df = pd.read_parquet(
        f"s3://{MINIO_BUCKET}/silver/payments.parquet",
        storage_options=STORAGE_OPTIONS
    )

    for _, row in df.iterrows():
        conn.execute(text("""
            INSERT INTO {SCHEMA}.fact_payments
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (:order_id, :payment_sequential, :payment_type, :payment_installments, CAST(:payment_value AS NUMERIC(10,2)))
            ON CONFLICT DO NOTHING;
        """.format(SCHEMA=SCHEMA)), row.to_dict())

    print(f"Đã load fact_payments: {len(df)} dòng")

def load_fact_reviews(conn):
    df = pd.read_parquet(f"s3://{MINIO_BUCKET}/silver/reviews.parquet",
                         storage_options=STORAGE_OPTIONS)
    for _, row in df.iterrows():
        conn.execute(text(f"""
            INSERT INTO {SCHEMA}.fact_reviews 
            (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date)
            VALUES (:review_id, :order_id, :review_score, :review_comment_title, :review_comment_message, NULL)
            ON CONFLICT (review_id) DO UPDATE
            SET review_score = EXCLUDED.review_score,
                review_comment_message = EXCLUDED.review_comment_message;
        """), row.to_dict())
    print(f"Da load fact_reviews: {len(df)} dong")

def load_dim_geolocation(conn):
    path = f"s3://{MINIO_BUCKET}/silver/geolocation.parquet"
    df = pd.read_parquet(path, storage_options=STORAGE_OPTIONS)


    for _, row in df.iterrows():
        conn.execute(text("""
            INSERT INTO olist_dw.dim_geolocation 
            (geolocation_zip_code_prefix, latitude, longitude, geolocation_city, geolocation_state)
            VALUES (:geolocation_zip_code_prefix, :latitude, :longitude, :geolocation_city, :geolocation_state)
            ON CONFLICT (geolocation_zip_code_prefix, geolocation_city, geolocation_state) DO NOTHING;
        """), row.to_dict())

    print(f"Da load Dim_geolocation: {len(df)} dong")

if __name__ == "__main__":
    with engine.begin() as conn:
        load_dim_date(conn)
        load_dim_customer(conn)
        load_dim_seller(conn)
        load_dim_product(conn)
        load_dim_geolocation(conn)

        load_fact_orders(conn)
        load_fact_order_items(conn)
        load_fact_payments(conn)
        load_fact_reviews(conn)

    print("Load silver → gold thành công ")
