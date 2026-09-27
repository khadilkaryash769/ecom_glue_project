import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------------------------------------------------
# Expected job arguments
# -------------------------------------------------------------------
# Required:
# --JOB_NAME
# --BRONZE_ORDERS_PATH
# --BRONZE_PRODUCTS_PATH
# --SILVER_CURATED_PATH
# --SILVER_REJECTED_PATH
# --PIPELINE_RUN_ID
#
# Example:
# --BRONZE_ORDERS_PATH s3://demo-medallion-etl-pipeline/bronze/orders/
# --BRONZE_PRODUCTS_PATH s3://demo-medallion-etl-pipeline/bronze/products/
# --SILVER_CURATED_PATH s3://demo-medallion-etl-pipeline/silver/orders_curated/
# --SILVER_REJECTED_PATH s3://demo-medallion-etl-pipeline/silver/rejected_orders/
# --PIPELINE_RUN_ID run_20260321_02

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_ORDERS_PATH",
        "BRONZE_PRODUCTS_PATH",
        "SILVER_CURATED_PATH",
        "SILVER_REJECTED_PATH",
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

bronze_orders_path = args["BRONZE_ORDERS_PATH"]
bronze_products_path = args["BRONZE_PRODUCTS_PATH"]
silver_curated_path = args["SILVER_CURATED_PATH"]
silver_rejected_path = args["SILVER_REJECTED_PATH"]
pipeline_run_id = args["PIPELINE_RUN_ID"]

# -------------------------------------------------------------------
# Read Bronze datasets
# -------------------------------------------------------------------
# Orders and products are already available in Parquet format in Bronze.
# We now apply data quality checks and build Silver outputs.
# -------------------------------------------------------------------
orders_df = spark.read.parquet(bronze_orders_path)
products_df = spark.read.parquet(bronze_products_path)

# -------------------------------------------------------------------
# Helper function:
# Convert blank strings into NULL so that validations become easier.
# Example:
# " " -> NULL
# ""  -> NULL
# -------------------------------------------------------------------
def blank_as_null(column_name):
    return F.when(F.trim(F.col(column_name)) == "", None).otherwise(F.col(column_name))

# -------------------------------------------------------------------
# STEP 1: Standardize Bronze Orders before validation
# -------------------------------------------------------------------
# What we do here:
# - convert blank business keys into NULL
# - standardize status using trim + lower
# - parse order_date string into timestamp
#
# Note:
# We are intentionally validating only one date format here:
# yyyy-MM-dd HH:mm:ss
# This makes the demo simple and helps show rejected records clearly.
# -------------------------------------------------------------------
orders_prepared_df = (
    orders_df
    .withColumn("order_id", blank_as_null("order_id"))
    .withColumn("customer_id", blank_as_null("customer_id"))
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("order_status_std", F.lower(F.trim(F.col("order_status"))))
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss"))
)

# -------------------------------------------------------------------
# STEP 2: Detect duplicate order_id values
# -------------------------------------------------------------------
# Business rule:
# - Keep the first occurrence of an order_id
# - Reject later duplicate occurrences
#
# We use row_number() over order_id to identify duplicates.
# Ordering is based on:
# - load_date
# - source_file_name
# - ingestion_timestamp
# -------------------------------------------------------------------
order_dup_window = Window.partitionBy("order_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

orders_with_dup_flag_df = (
    orders_prepared_df
    .withColumn("dup_rank", F.row_number().over(order_dup_window))
)

# -------------------------------------------------------------------
# STEP 3: Apply Data Quality Rules on Orders
# -------------------------------------------------------------------
# Rules being checked:
# - order_id must not be null
# - customer_id must not be null
# - product_id must not be null
# - order_timestamp must be valid
# - quantity must be > 0
# - unit_price must be > 0
# - order_status must be one of:
#       completed, pending, cancelled
# - duplicate order_id beyond first occurrence is rejected
#
# We create a rejection_reason column.
# If rejection_reason is NULL, the record is considered valid.
# -------------------------------------------------------------------
orders_validated_df = (
    orders_with_dup_flag_df
    .withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID"))
         .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
         .when(F.col("product_id").isNull(), F.lit("NULL_PRODUCT_ID"))
         .when(F.col("order_timestamp").isNull(), F.lit("INVALID_ORDER_DATE"))
         .when(F.col("quantity").isNull(), F.lit("NULL_QUANTITY"))
         .when(F.col("quantity") <= 0, F.lit("INVALID_QUANTITY"))
         .when(F.col("unit_price").isNull(), F.lit("NULL_UNIT_PRICE"))
         .when(F.col("unit_price") <= 0, F.lit("INVALID_UNIT_PRICE"))
         .when(~F.col("order_status_std").isin("completed", "pending", "cancelled"), F.lit("INVALID_ORDER_STATUS"))
         .when((F.col("order_id").isNotNull()) & (F.col("dup_rank") > 1), F.lit("DUPLICATE_ORDER_ID"))
         .otherwise(F.lit(None))
    )
)

# -------------------------------------------------------------------
# STEP 4: Separate rejected and valid orders
# -------------------------------------------------------------------
# Rejected records are written to silver/rejected_orders.
# We store the original raw-like order columns as a JSON string inside raw_record.
# This makes debugging and demo explanation easier.
# -------------------------------------------------------------------
rejected_orders_df = (
    orders_validated_df
    .filter(F.col("rejection_reason").isNotNull())
    .select(
        F.to_json(
            F.struct(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("product_id"),
                F.col("order_date"),
                F.col("quantity"),
                F.col("unit_price"),
                F.col("order_status")
            )
        ).alias("raw_record"),
        F.col("rejection_reason"),
        F.col("source_file_name"),
        F.col("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

valid_orders_df = (
    orders_validated_df
    .filter(F.col("rejection_reason").isNull())
)

# -------------------------------------------------------------------
# STEP 5: Clean Product Master
# -------------------------------------------------------------------
# Product master is used for enrichment, so we first clean it.
#
# What we do here:
# - convert blank strings to NULL
# - keep only valid product records where:
#     product_id, product_name, category, brand are all present
# -------------------------------------------------------------------
products_prepared_df = (
    products_df
    .withColumn("product_id", blank_as_null("product_id"))
    .withColumn("product_name", blank_as_null("product_name"))
    .withColumn("category", blank_as_null("category"))
    .withColumn("brand", blank_as_null("brand"))
)

products_valid_df = (
    products_prepared_df
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("product_name").isNotNull())
    .filter(F.col("category").isNotNull())
    .filter(F.col("brand").isNotNull())
)

# -------------------------------------------------------------------
# STEP 6: Deduplicate Product Master on product_id
# -------------------------------------------------------------------
# If multiple valid product records exist for the same product_id,
# we keep the first one based on:
# - load_date
# - source_file_name
# - ingestion_timestamp
# -------------------------------------------------------------------
product_dup_window = Window.partitionBy("product_id").orderBy(
    F.col("load_date").asc(),
    F.col("source_file_name").asc(),
    F.col("ingestion_timestamp").asc()
)

products_dedup_df = (
    products_valid_df
    .withColumn("prod_rank", F.row_number().over(product_dup_window))
    .filter(F.col("prod_rank") == 1)
    .select(
        F.col("product_id").alias("p_product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand")
    )
)

# -------------------------------------------------------------------
# STEP 7: Enrich valid orders with cleaned product master
# -------------------------------------------------------------------
# We perform a LEFT JOIN because some orders may have product_id values
# that are not present in valid product master.
#
# In such cases we still keep the order in Silver and populate:
# - product_name = UNKNOWN
# - category     = UNKNOWN
# - brand        = UNKNOWN
#
# This is useful for demonstration because:
# - the order is still valid from transaction perspective
# - the enrichment lookup failed
# -------------------------------------------------------------------
orders_curated_df = (
    valid_orders_df.alias("o")
    .join(
        products_dedup_df.alias("p"),
        F.col("o.product_id") == F.col("p.p_product_id"),
        "left"
    )
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.product_id").alias("product_id"),
        F.coalesce(F.col("p.product_name"), F.lit("UNKNOWN")).alias("product_name"),
        F.coalesce(F.col("p.category"), F.lit("UNKNOWN")).alias("category"),
        F.coalesce(F.col("p.brand"), F.lit("UNKNOWN")).alias("brand"),
        F.col("o.order_timestamp").alias("order_timestamp"),
        F.to_date(F.col("o.order_timestamp")).alias("order_date"),
        F.col("o.quantity").cast("int").alias("quantity"),
        F.col("o.unit_price").cast("double").alias("unit_price"),
        (F.col("o.quantity") * F.col("o.unit_price")).cast("double").alias("order_amount"),
        F.col("o.order_status_std").alias("order_status"),
        F.col("o.load_date").alias("load_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id")
    )
)

# -------------------------------------------------------------------
# STEP 8: Write Silver Curated Orders
# -------------------------------------------------------------------
# Output path:
# s3://.../silver/orders_curated/
#
# Partitioning by load_date keeps the folder structure easy to query
# and explain during the demo.
# -------------------------------------------------------------------
orders_curated_df.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("load_date") \
    .save(silver_curated_path)

# -------------------------------------------------------------------
# STEP 9: Write Silver Rejected Orders
# -------------------------------------------------------------------
# Output path:
# s3://.../silver/rejected_orders/
# -------------------------------------------------------------------
rejected_orders_df.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("load_date") \
    .save(silver_rejected_path)

# -------------------------------------------------------------------
# Commit Glue job
# -------------------------------------------------------------------
job.commit()