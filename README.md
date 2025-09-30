E-Commerce Data Pipeline (ELT Project)
📌 Project Overview

This project implements an ELT (Extract – Load – Transform) pipeline to process and analyze e-commerce data from the Brazilian Olist dataset.
The system is designed as part of my academic coursework in Data Engineering, focusing on building a modern data pipeline for data integration, cleaning, warehousing, and visualization.

🏗️ Architecture

The pipeline follows a layered architecture:

Data Lake (MinIO)

Raw CSV data ingested into object storage (Bronze layer).

Cleaned and standardized Parquet files stored in Silver layer.

Transformation (Python & Pandas)

Data cleaning and normalization scripts.

Handling duplicates, missing values, and type conversions.

Data Warehouse (PostgreSQL)

Star schema with fact and dimension tables.

Optimized for analytical queries and BI reporting.

Visualization & Access Layer

Power BI Dashboards: sales, orders, customers, and product insights.

Flask API (prototype) for data access.

🔧 Technologies Used

Python (Pandas, PyArrow, SQLAlchemy)

MinIO (S3-compatible object storage)

PostgreSQL (Data Warehouse)

Power BI (Data visualization)

Flask (API layer)

Docker (for containerization, optional)

📊 Key Features

End-to-end ELT pipeline from raw data to visualization.

Data lake with Bronze/Silver/Gold layers.

Cleaned and transformed data for analytical workloads.

Power BI dashboards for business insights.
