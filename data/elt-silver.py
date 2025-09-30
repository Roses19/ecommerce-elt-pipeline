import pandas as pd
import numpy as np
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, MINIO_BUCKET
import json   

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS,
    "secret": MINIO_SECRET,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
}

def clean_orders():
    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_orders_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    # 1. ep kieu datetime
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    # 2. them vung (partition) date
    df["order_purchase_date"] = df["order_purchase_timestamp"].dt.date

    # 3. rang buoc order_status
    status_map = {
        "delivered": "delivered",
        "shipped": "shipped",
        "canceled": "canceled",
        "unavailable": "canceled",  
        "invoiced": "approved",   
        "processing": "created",    
        "created": "created",
        "approved": "approved",
    }
    df["order_status"] = df["order_status"].map(status_map).fillna("invalid")

    # status_flag: 1 hợp lệ, 0 invalid
    df["status_flag"] = df["order_status"].apply(lambda x: 0 if x == "invalid" else 1)

    # 4. logic thoi gian
    def check_time(row):
        try:
            if pd.isna(row["order_purchase_timestamp"]):
                return 1
            if pd.notna(row["order_approved_at"]) and row["order_purchase_timestamp"] > row["order_approved_at"]:
                return 1
            if pd.notna(row["order_delivered_carrier_date"]) and row["order_approved_at"] and row["order_approved_at"] > row["order_delivered_carrier_date"]:
                return 1
            if pd.notna(row["order_delivered_customer_date"]) and row["order_delivered_carrier_date"] and row["order_delivered_carrier_date"] > row["order_delivered_customer_date"]:
                return 1
            if pd.notna(row["order_estimated_delivery_date"]) and row["order_delivered_customer_date"] and row["order_delivered_customer_date"] > row["order_estimated_delivery_date"]:
                return 1
            return 0
        except:
            return 1

    df["time_anomaly"] = df.apply(check_time, axis=1)

    #flag đã giao
    df["delivered_flag"] = df["order_delivered_customer_date"].apply(lambda x: 1 if pd.notna(x) else 0)

    # 5. dam bao order_id duy nhat
    df = df.sort_values("order_purchase_timestamp").drop_duplicates(subset=["order_id"], keep="last")

    # save parquet → silver
    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/orders.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )
    print(f"Orders cleaned: {df.shape[0]} rows → silver/")

    return df.head()

def clean_customers():

    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_customers_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"]
        .astype(str)
        .str.zfill(5)
    )

    df["customer_city"] = df["customer_city"].str.lower().str.strip()
    df["customer_state"] = df["customer_state"].str.upper()


    state_map = {
        "AC": "Acre","AL": "Alagoas","AP": "Amapá","AM": "Amazonas","BA": "Bahia","CE": "Ceará",
        "DF": "Distrito Federal (Brasília)","ES": "Espírito Santo","GO": "Goiás","MA": "Maranhão",
        "MT": "Mato Grosso","MS": "Mato Grosso do Sul","MG": "Minas Gerais","PA": "Pará","PB": "Paraíba",
        "PR": "Paraná","PE": "Pernambuco","PI": "Piauí","RJ": "Rio de Janeiro","RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul","RO": "Rondônia","RR": "Roraima","SC": "Santa Catarina",
        "SP": "São Paulo","SE": "Sergipe","TO": "Tocantins"
    }
    df["customer_state_name"] = df["customer_state"].map(state_map)


    df_id = df.drop_duplicates(subset=["customer_id"], keep="first")
    df_id.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/customers.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )


    df_unique = df.drop_duplicates(subset=["customer_unique_id"], keep="first")
    df_unique.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/customers_unique.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print(f"Customers cleaned: {df_id.shape[0]} rows (customers) → silver/")
    print(f"Unique customers cleaned: {df_unique.shape[0]} rows (unique customers) → silver/")

    return df_id.head(), df_unique.head()

def clean_order_items():

    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_order_items_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce").dt.normalize()
    df = df.drop_duplicates(subset=[
        "order_id", "order_item_id", "product_id", "seller_id"
    ])

    df.to_parquet(f"s3://{MINIO_BUCKET}/silver/order_items.parquet", 
                  storage_options=STORAGE_OPTIONS, index=False)

    print(f"Order_items cleaned: {df.shape[0]} rows → silver/")

    return df.head()

def clean_products():
    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_products_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    try:
        trans = pd.read_csv(
            f"s3://{MINIO_BUCKET}/raw/product_category_name_translation.csv",
            storage_options=STORAGE_OPTIONS
        )
        df = df.merge(trans, on="product_category_name", how="left")
        df["product_category_name"] = df["product_category_name_english"].fillna(df["product_category_name"])
        df.drop(columns=["product_category_name_english"], inplace=True)
    except Exception as e:
        print("Không tìm thấy file translation:", e)

    df.rename(columns={
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length"
    }, inplace=True)

    num_cols = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col] > 0, None)

    df["size_anomaly"] = df["product_weight_g"].apply(
        lambda x: True if pd.notna(x) and x < 50 else False
    )

    df["product_volume_cm3"] = (
        df["product_length_cm"] * df["product_height_cm"] * df["product_width_cm"]
    )

    df = df.drop_duplicates(subset=["product_id"])

    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/products.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print(f"Products cleaned: {df.shape[0]} rows → silver/")
    return df.head()

def clean_sellers():
    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_sellers_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    df = df.drop_duplicates(subset=["seller_id"])


    df["seller_city"] = df["seller_city"].str.strip().str.title()

    df["seller_state"] = df["seller_state"].str.upper()
    df.loc[df["seller_state"].str.len() != 2, "seller_state"] = "Unknown"

    df["seller_zip_code_prefix"] = ( df["seller_zip_code_prefix"].astype(str).str.zfill(5))

    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/sellers.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print("sellers → silver (cleaned)")

    return df.head()

def clean_payments():

    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_order_payments_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )


    df = df.drop_duplicates()

    df["payment_type"] = df["payment_type"].fillna("unknown")
    df["payment_type"] = df["payment_type"].replace("not_defined", "unknown")


    df.loc[df["payment_installments"] <= 0, "payment_installments"] = 1


    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df.loc[df["payment_value"] <= 0, "payment_value"] = np.nan


    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/payments.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print("payments → silver (cleaned)")
    return df.head()

def clean_reviews():

    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_order_reviews_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce", utc=True)
    df["review_answer_timestamp"] = pd.to_datetime(df["review_answer_timestamp"], errors="coerce", utc=True)

    df = df.sort_values("review_answer_timestamp")
    df = df.drop_duplicates(subset=["review_id"], keep="last")

    df = df.drop_duplicates(subset=["order_id"], keep="last")


    for col in ["review_comment_title", "review_comment_message"]:
        df[col] = df[col].astype(str).str.strip().replace("nan", np.nan)

    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/reviews.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print("reviews → silver (cleaned)")
    return df.head()


def clean_geolocation():
    df = pd.read_csv(
        f"s3://{MINIO_BUCKET}/raw/olist_geolocation_dataset.csv",
        storage_options=STORAGE_OPTIONS
    )

    before = df.shape[0]

    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"Đã loại bỏ {before - after} bản ghi trùng lặp hoàn toàn")


    df["geolocation_city"] = (
        df["geolocation_city"]
        .str.lower()
        .str.strip()
    )
    df["geolocation_zip_code_prefix"] = (
        df["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    )

    df = (
        df.groupby(["geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"])
        .agg({
            "geolocation_lat": "mean",
            "geolocation_lng": "mean"
        })
        .reset_index()
    )

    df.rename(columns={
        "geolocation_lat": "latitude",
        "geolocation_lng": "longitude"
    }, inplace=True)

    df.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/geolocation.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )

    print(f"geolocation → silver (cleaned & unique), còn lại {df.shape[0]} rows")
    return df.head()

def normalize_sample(val):
    if isinstance(val, pd.Series) or isinstance(val, np.ndarray):
        val = val.tolist()
    if val is None:
        val = []
    if not isinstance(val, str):
        val = json.dumps(val, ensure_ascii=False)
    return val


def validate_data(tables: dict):
    dq_checks = []

    orders = tables["orders"]
    customers = tables["customers"]
    order_items = tables["order_items"]
    products = tables["products"]
    sellers = tables["sellers"]
    payments = tables["payments"]
    reviews = tables["reviews"]
    geolocation = tables["geolocation"]

    invalid_orders = orders[~orders["customer_id"].isin(customers["customer_id"])]
    dq_checks.append({
        "check_name": "orders_customer_fk",
        "table_name": "orders",
        "status": "FAILED" if not invalid_orders.empty else "PASSED",
        "num_violations": len(invalid_orders),
        "sample_records": invalid_orders["order_id"].head(5).tolist()
    })

    invalid_items_order = order_items[~order_items["order_id"].isin(orders["order_id"])]
    dq_checks.append({
        "check_name": "order_items_order_fk",
        "table_name": "order_items",
        "status": "FAILED" if not invalid_items_order.empty else "PASSED",
        "num_violations": len(invalid_items_order),
        "sample_records": invalid_items_order["order_id"].head(5).tolist()
    })

    invalid_items_seller = order_items[~order_items["seller_id"].isin(sellers["seller_id"])]
    dq_checks.append({
        "check_name": "order_items_seller_fk",
        "table_name": "order_items",
        "status": "FAILED" if not invalid_items_seller.empty else "PASSED",
        "num_violations": len(invalid_items_seller),
        "sample_records": invalid_items_seller["seller_id"].head(5).tolist()
    })

    invalid_items_product = order_items[~order_items["product_id"].isin(products["product_id"])]
    dq_checks.append({
        "check_name": "order_items_product_fk",
        "table_name": "order_items",
        "status": "FAILED" if not invalid_items_product.empty else "PASSED",
        "num_violations": len(invalid_items_product),
        "sample_records": invalid_items_product["product_id"].head(5).tolist()
    })

    invalid_payments = payments[~payments["order_id"].isin(orders["order_id"])]
    dq_checks.append({
        "check_name": "payments_order_fk",
        "table_name": "order_payments",
        "status": "FAILED" if not invalid_payments.empty else "PASSED",
        "num_violations": len(invalid_payments),
        "sample_records": invalid_payments["order_id"].head(5).tolist()
    })

    invalid_reviews = reviews[~reviews["order_id"].isin(orders["order_id"])]
    dq_checks.append({
        "check_name": "reviews_order_fk",
        "table_name": "order_reviews",
        "status": "FAILED" if not invalid_reviews.empty else "PASSED",
        "num_violations": len(invalid_reviews),
        "sample_records": invalid_reviews["order_id"].head(5).tolist()
    })


    geo_zips = set(geolocation["geolocation_zip_code_prefix"].unique())


    customers["zip_flag"] = customers["customer_zip_code_prefix"].isin(geo_zips).astype(int)
    invalid_customers_zip = customers[customers["zip_flag"] == 0]

    dq_checks.append({
        "check_name": "customers_zip_coverage",
        "table_name": "customers",
        "status": "FAILED" if len(invalid_customers_zip) > 0 else "PASSED",
        "num_violations": len(invalid_customers_zip),
        "coverage_rate": round(customers["zip_flag"].mean() * 100, 2),  # %
        "sample_records": invalid_customers_zip["customer_id"].head(5).tolist()
    })

    sellers["zip_flag"] = sellers["seller_zip_code_prefix"].isin(geo_zips).astype(int)
    invalid_sellers_zip = sellers[sellers["zip_flag"] == 0]

    dq_checks.append({
        "check_name": "sellers_zip_coverage",
        "table_name": "sellers",
        "status": "FAILED" if len(invalid_sellers_zip) > 0 else "PASSED",
        "num_violations": len(invalid_sellers_zip),
        "coverage_rate": round(sellers["zip_flag"].mean() * 100, 2),
        "sample_records": invalid_sellers_zip["seller_id"].head(5).tolist()
    })


    # Consistency: đối chiếu tiền
    items_sum = order_items.groupby("order_id")[["price","freight_value"]].sum().sum(axis=1)
    payments_sum = payments.groupby("order_id")["payment_value"].sum()
    amount_check = pd.concat([items_sum, payments_sum], axis=1).fillna(0)
    amount_check.columns = ["sum_items", "sum_payments"]
    amount_check["diff"] = (amount_check["sum_items"] - amount_check["sum_payments"]).abs()
    mismatch_orders = amount_check[amount_check["diff"] > 1e-2]
    dq_checks.append({
        "check_name": "amount_consistency",
        "table_name": "orders",
        "status": "FAILED" if not mismatch_orders.empty else "PASSED",
        "num_violations": len(mismatch_orders),
        "sample_records": mismatch_orders.index[:5].tolist()
    })

    for rec in dq_checks:
        rec["sample_records"] = normalize_sample(rec.get("sample_records"))

    dq_report = pd.DataFrame(dq_checks)
    return dq_report

def load_silver_tables():
    silver_paths = {
        "orders": "orders.parquet",
        "customers": "customers.parquet",
        "order_items": "order_items.parquet",
        "products": "products.parquet",
        "sellers": "sellers.parquet",
        "payments": "payments.parquet",
        "reviews": "reviews.parquet",
        "geolocation": "geolocation.parquet",
    }

    tables = {}
    for name, file in silver_paths.items():
        tables[name] = pd.read_parquet(
            f"s3://{MINIO_BUCKET}/silver/{file}",
            storage_options=STORAGE_OPTIONS
        )
    return tables

if __name__ == "__main__":
    clean_orders()
    clean_customers()
    clean_order_items()
    clean_products()
    clean_sellers()
    clean_payments()
    clean_reviews()
    clean_geolocation()
    print("All raw → silver done.")

    tables = load_silver_tables()

    dq_report = validate_data(tables)

    dq_report.to_parquet(
        f"s3://{MINIO_BUCKET}/silver/reports/data_quality_report.parquet",
        index=False,
        storage_options=STORAGE_OPTIONS
    )
    print(f"Bao cao chat luong du lieu luu: {dq_report.shape[0]} checks → silver/reports/")
