# E-commerce Serverless ETL Pipeline (Medallion Architecture)

This repository contains an end-to-end serverless data pipeline designed for an e-commerce platform using the **Medallion Architecture**. The project is built using AWS cloud-native services to process raw orders and products data, moving it through Bronze, Silver, and Gold layers to enable high-performance analytics.

## 🛠️ Tech Stack & Architecture
* **Data Storage:** Amazon S3 (Bronze, Silver, Gold Buckets)
* **ETL & Processing:** AWS Glue (PySpark / Serverless Spark)
* **Data Cataloging:** AWS Glue Data Catalog & Glue Crawlers
* **Ad-hoc Analytics:** Amazon Athena (SQL queries for Gold layer)

---

## 🏗️ Pipeline Breakdown (Medallion Layers)

1. **Bronze Layer (Raw Ingestion):**
   * Raw JSON data (`orders` and `products`) is ingested directly into the Bronze S3 bucket as-is.
   * **Goal:** Maintains strict data lineage and an immutable audit trail of the original raw data.

2. **Silver Layer (Cleaned & Standardized):**
   * Data from the Bronze layer is processed using PySpark in AWS Glue.
   * **Actions:** Performed deduplication, validated schemas, handled missing/null values, and flattened complex nested JSON structures.
   * **Format Optimization:** Converted raw data into compressed **Parquet format** for high performance and cost reduction in analytical querying.

3. **Gold Layer (Business Aggregations):**
   * Cleaned tables are joined (`orders` joined with `products`) to create business-level aggregates.
   * **Goal:** Pre-aggregating data for faster business reporting, ready to be directly consumed by BI tools or Amazon Athena.

---

## 📂 Project Structure
* `01_prepare_s3_structure` - Setup script for Amazon S3 directory layout.
* `02_layer_wise_schema` - Pre-defined schemas mapped to each medallion tier.
* `03_raw_to_bronze.py` - AWS Glue job handling raw ingestion.
* `04_bronze_to_silver.py` - AWS Glue job handling deduplication, cleaning, and Parquet conversion.
* `05_silver_to_gold.py` - AWS Glue job performing star-schema joins and aggregations.

---

## 📊 Analytics & Querying
After the Gold layer job finishes, **AWS Glue Crawlers** scan the optimized S3 buckets to update the **Glue Data Catalog**. Business analyst teams can then run instant, low-cost SQL queries via **Amazon Athena** directly on top of the Gold datasets.
