import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -------------------------------------------------------------------
# Expected job arguments
# -------------------------------------------------------------------
# Required:
# --JOB_NAME
# --SILVER_CURATED_PATH
# --GOLD_DAILY_PRODUCT_SALES_PATH
# --GOLD_CATEGORY_SALES_PATH
# --PIPELINE_RUN_ID
#
# Example:
# --SILVER_CURATED_PATH s3://demo-medallion-etl-pipeline/silver/orders_curated/
# --GOLD_DAILY_PRODUCT_SALES_PATH s3://demo-medallion-etl-pipeline/gold/daily_product_sales/
# --GOLD_CATEGORY_SALES_PATH s3://demo-medallion-etl-pipeline/gold/category_sales/
# --PIPELINE_RUN_ID run_20260321_03

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_CURATED_PATH",
        "GOLD_DAILY_PRODUCT_SALES_PATH",
        "GOLD_CATEGORY_SALES_PATH",
        "PIPELINE_RUN_ID"
    ]
)

# -------------------------------------------------------------------
# Initialize Spark / Glue contexts
# -------------------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

silver_curated_path = args["SILVER_CURATED_PATH"]
gold_daily_product_sales_path = args["GOLD_DAILY_PRODUCT_SALES_PATH"]
gold_category_sales_path = args["GOLD_CATEGORY_SALES_PATH"]
pipeline_run_id = args["PIPELINE_RUN_ID"]

# -------------------------------------------------------------------
# Important setting:
# Use dynamic partition overwrite so that when a partition
# (for example order_date=2026-03-20) is reprocessed,
# only that partition is refreshed instead of replacing all data.
# -------------------------------------------------------------------
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -------------------------------------------------------------------
# STEP 1: Read Silver Curated Orders
# -------------------------------------------------------------------
# This dataset already contains:
# - cleaned order data
# - standardized status
# - derived order_amount
# - enriched product attributes
# -------------------------------------------------------------------
orders_curated_df = spark.read.parquet(silver_curated_path)

# -------------------------------------------------------------------
# STEP 2: Apply safety filters before Gold aggregation
# -------------------------------------------------------------------
# Silver is already curated, but we still apply a few safeguards
# before creating business aggregates.
#
# What we check here:
# - order_id must not be null
# - order_date must not be null
# - quantity must not be null
# - unit_price must not be null
# - order_amount must not be null
#
# This protects Gold from accidental bad rows.
# -------------------------------------------------------------------
gold_base_df = (
    orders_curated_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("unit_price").isNotNull())
    .filter(F.col("order_amount").isNotNull())
)

# -------------------------------------------------------------------
# STEP 3: Build Gold Table - Daily Product Sales
# -------------------------------------------------------------------
# Aggregation level:
# - one row per order_date + product_id + product_name + category
#
# Measures:
# - total_orders   -> distinct order count
# - total_quantity -> total quantity sold
# - total_sales    -> total order_amount
#
# This table is useful for:
# - daily product-wise reporting
# - product trend analysis
# - dashboarding
# -------------------------------------------------------------------
daily_product_sales_df = (
    gold_base_df
    .groupBy(
        F.col("order_date"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("category")
    )
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
)

# -------------------------------------------------------------------
# STEP 4: Build Gold Table - Category Sales
# -------------------------------------------------------------------
# Aggregation level:
# - one row per order_date + category
#
# Measures:
# - total_orders
# - total_quantity
# - total_sales
#
# This table is useful for:
# - daily category-wise reporting
# - management summary
# - comparing category performance
# -------------------------------------------------------------------
category_sales_df = (
    gold_base_df
    .groupBy(
        F.col("order_date"),
        F.col("category")
    )
    .agg(
        F.countDistinct("order_id").cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_quantity"),
        F.round(F.sum("order_amount"), 2).cast("double").alias("total_sales")
    )
)

# -------------------------------------------------------------------
# STEP 5: Write Gold Daily Product Sales
# -------------------------------------------------------------------
# Output path:
# s3://.../gold/daily_product_sales/
#
# We partition by order_date because Gold is a reporting layer,
# and date-wise partitioning makes the data easier to query and refresh.
#
# Overwrite mode + dynamic partition overwrite:
# only affected partitions get refreshed.
# -------------------------------------------------------------------
daily_product_sales_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("order_date") \
    .save(gold_daily_product_sales_path)

# -------------------------------------------------------------------
# STEP 6: Write Gold Category Sales
# -------------------------------------------------------------------
# Output path:
# s3://.../gold/category_sales/
# -------------------------------------------------------------------
category_sales_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("order_date") \
    .save(gold_category_sales_path)

# -------------------------------------------------------------------
# Commit Glue job
# -------------------------------------------------------------------
job.commit()