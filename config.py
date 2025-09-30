import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=False)

MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "e-commerce")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

PG_URL         = os.getenv("PG_URL", "postgresql+psycopg2://roses:roses@localhost:5433/olist_project")
PG_SCHEMA      = os.getenv("PG_SCHEMA", "olist_dw")
