"""
Glue Job 1: Raw load
  S3 raw CSV → transform → S3 curated Parquet → Redshift stage → UPSERT target

Job parameters (set in Glue console):
  --S3_SOURCE_PATH      s3://bucket/raw/
  --S3_CURATED_PATH     s3://bucket/curated/sales/
  --REDSHIFT_CONNECTION heena-sales-redshift-connection
  --REDSHIFT_DATABASE   dev
  --REDSHIFT_TMP_DIR    s3://bucket/tmp/
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_SOURCE_PATH",
        "S3_CURATED_PATH",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_DATABASE",
        "REDSHIFT_TMP_DIR",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

SCHEMA = "public"
STAGE_TABLE = "stage_sales"
TARGET_TABLE = "target_sales"

print("=" * 60)
print("JOB 1: Raw load — S3 → Parquet → Redshift UPSERT")
print("=" * 60)

# Step 1: Read CSV from S3
print(f"[1] Reading: {args['S3_SOURCE_PATH']}")
df = spark.read.option("header", True).option("inferSchema", False).csv(args["S3_SOURCE_PATH"])
count = df.count()
print(f"    Records read: {count}")

if count == 0:
    raise RuntimeError("No records in source CSV")

# Step 2: Rename columns + cast types (CSV headers → table columns)
print("[2] Transforming")
df_clean = df.selectExpr(
    "CAST(uuid AS BIGINT) AS uuid",
    "Country AS country",
    "ItemType AS item_type",
    "SalesChannel AS sales_channel",
    "OrderPriority AS order_priority",
    "OrderDate AS order_date",
    "Region AS region",
    "ShipDate AS ship_date",
    "CAST(UnitsSold AS INT) AS units_sold",
    "CAST(UnitPrice AS DECIMAL(10,2)) AS unit_price",
    "CAST(UnitCost AS DECIMAL(10,2)) AS unit_cost",
    "CAST(TotalRevenue AS DECIMAL(15,2)) AS total_revenue",
    "CAST(TotalCost AS DECIMAL(15,2)) AS total_cost",
    "CAST(TotalProfit AS DECIMAL(15,2)) AS total_profit",
)

# Step 3: Write silver layer (Parquet)
print(f"[3] Writing Parquet: {args['S3_CURATED_PATH']}")
df_clean.write.mode("overwrite").parquet(args["S3_CURATED_PATH"])

# Step 4: Load stage → UPSERT target
print("[4] Redshift load (stage → UPSERT → target)")
dynamic_frame = DynamicFrame.fromDF(df_clean, glue_context, "sales")

pre_actions = f"TRUNCATE TABLE {SCHEMA}.{STAGE_TABLE};"
post_actions = (
    f"DELETE FROM {SCHEMA}.{TARGET_TABLE} "
    f"USING {SCHEMA}.{STAGE_TABLE} "
    f"WHERE {SCHEMA}.{TARGET_TABLE}.uuid = {SCHEMA}.{STAGE_TABLE}.uuid; "
    f"INSERT INTO {SCHEMA}.{TARGET_TABLE} "
    f"SELECT * FROM {SCHEMA}.{STAGE_TABLE};"
)

glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection=args["REDSHIFT_CONNECTION"],
    connection_options={
        "database": args["REDSHIFT_DATABASE"],
        "dbtable": f"{SCHEMA}.{STAGE_TABLE}",
        "preactions": pre_actions,
        "postactions": post_actions,
    },
    redshift_tmp_dir=args["REDSHIFT_TMP_DIR"],
)

print("[5] Done — check target_sales for 899 rows")
print("=" * 60)

sc.stop()
