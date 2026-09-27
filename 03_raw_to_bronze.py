import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    lit,
    regexp_extract
)

# -------------------------------------------------------------------
# Expected job arguments
# -------------------------------------------------------------------
# Required:
# --JOB_NAME
# --ORDERS_SOURCE_PATH
# --PRODUCTS_SOURCE_PATH
# --BRONZE_ORDERS_TARGET_PATH
# --BRONZE_PRODUCTS_TARGET_PATH
# --PIPELINE_RUN_ID
# --LOAD_DATE_MODE
#
# Optional:
# --LOAD_DATE_VALUE   -> used only when LOAD_DATE_MODE = STATIC
#
# Example:
# --ORDERS_SOURCE_PATH s3://demo-medallion-etl-pipeline/raw/orders/
# --PRODUCTS_SOURCE_PATH s3://demo-medallion-etl-pipeline/raw/products/
# --BRONZE_ORDERS_TARGET_PATH s3://demo-medallion-etl-pipeline/bronze/orders/
# --BRONZE_PRODUCTS_TARGET_PATH s3://demo-medallion-etl-pipeline/bronze/products/
# --PIPELINE_RUN_ID run_20260320_01
# --LOAD_DATE_MODE FROM_PATH

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ORDERS_SOURCE_PATH",
        "PRODUCTS_SOURCE_PATH",
        "BRONZE_ORDERS_TARGET_PATH",
        "BRONZE_PRODUCTS_TARGET_PATH",
        "PIPELINE_RUN_ID",
        "LOAD_DATE_MODE"
    ]
)

# Optional argument handling
load_date_value = None
if "--LOAD_DATE_VALUE" in sys.argv:
    extra_args = getResolvedOptions(sys.argv, ["LOAD_DATE_VALUE"])
    load_date_value = extra_args["LOAD_DATE_VALUE"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

orders_source_path = args["ORDERS_SOURCE_PATH"]
products_source_path = args["PRODUCTS_SOURCE_PATH"]
bronze_orders_target_path = args["BRONZE_ORDERS_TARGET_PATH"]
bronze_products_target_path = args["BRONZE_PRODUCTS_TARGET_PATH"]
pipeline_run_id = args["PIPELINE_RUN_ID"]
load_date_mode = args["LOAD_DATE_MODE"]


def add_metadata_columns(raw_df):
    df = (
        raw_df.withColumn(
            "source_file_name",
            regexp_extract(input_file_name(), r"([^/]+$)", 1)
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", lit(pipeline_run_id))
    )

    if load_date_mode == "FROM_PATH":
        df = df.withColumn(
            "load_date",
            regexp_extract(
                input_file_name(),
                r"load_date=([0-9]{4}-[0-9]{2}-[0-9]{2})",
                1
            )
        )

    elif load_date_mode == "STATIC":
        if not load_date_value:
            raise ValueError("LOAD_DATE_VALUE is required when LOAD_DATE_MODE = STATIC")
        df = df.withColumn("load_date", lit(load_date_value))

    elif load_date_mode == "CURRENT_DATE":
        current_date_str = datetime.now().strftime("%Y-%m-%d")
        df = df.withColumn("load_date", lit(current_date_str))

    else:
        raise ValueError("LOAD_DATE_MODE must be one of: FROM_PATH, STATIC, CURRENT_DATE")

    return df


# -------------------------------------------------------------------
# Read raw orders JSON
# -------------------------------------------------------------------
raw_orders_df = spark.read.json(orders_source_path)

bronze_orders_df = add_metadata_columns(raw_orders_df)

# -------------------------------------------------------------------
# Write Bronze Orders as Parquet
# -------------------------------------------------------------------
bronze_orders_df.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("load_date") \
    .save(bronze_orders_target_path)

# -------------------------------------------------------------------
# Read raw products JSON
# -------------------------------------------------------------------
raw_products_df = spark.read.json(products_source_path)

bronze_products_df = add_metadata_columns(raw_products_df)

# -------------------------------------------------------------------
# Write Bronze Products as Parquet
# -------------------------------------------------------------------
bronze_products_df.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("load_date") \
    .save(bronze_products_target_path)

job.commit()