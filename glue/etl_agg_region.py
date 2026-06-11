"""
Glue Job 2: Aggregate by region
  target_sales → group by region → stage → UPSERT target_sales_by_region

Expected result: 7 rows
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from py4j.java_gateway import java_import
from pyspark.context import SparkContext
from pyspark.sql.functions import count, sum as spark_sum

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "REDSHIFT_CONNECTION", "REDSHIFT_DATABASE", "REDSHIFT_TMP_DIR"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

SCHEMA = "public"
SOURCE = "target_sales"
STAGE = "stage_sales_by_region"
TARGET = "target_sales_by_region"

print("=" * 60)
print("JOB 2: Aggregate by region")
print("=" * 60)

# Step 1: Read cleaned sales from Redshift
connection = glue_context.extract_jdbc_conf(args["REDSHIFT_CONNECTION"])
jdbc_url = connection["url"] + "/" + args["REDSHIFT_DATABASE"]
user = connection["user"]
password = connection["password"]

jdbc_opts = {
    "url": jdbc_url,
    "user": user,
    "password": password,
    "driver": "com.amazon.redshift.jdbc42.Driver",
}

print(f"[1] Reading {SCHEMA}.{SOURCE}")
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_opts["url"])
    .option("dbtable", f"{SCHEMA}.{SOURCE}")
    .option("user", jdbc_opts["user"])
    .option("password", jdbc_opts["password"])
    .option("driver", jdbc_opts["driver"])
    .load()
)

if df.count() == 0:
    print("[WARN] No data in target_sales — run Job 1 first")
    sc.stop()
    sys.exit(0)

# Step 2: Aggregate
print("[2] Grouping by region")
df_agg = df.groupBy("region").agg(
    spark_sum("total_revenue").alias("total_revenue"),
    spark_sum("total_cost").alias("total_cost"),
    spark_sum("total_profit").alias("total_profit"),
    spark_sum("units_sold").alias("total_units_sold"),
    count("*").alias("order_count"),
)
print(f"    Regions: {df_agg.count()}")

# Step 3: Load stage
print(f"[3] Writing {SCHEMA}.{STAGE}")
df_agg.write.format("jdbc").option("url", jdbc_opts["url"]).option(
    "dbtable", f"{SCHEMA}.{STAGE}"
).option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option(
    "driver", jdbc_opts["driver"]
).option(
    "truncate", "true"
).mode(
    "overwrite"
).save()

# Step 4: UPSERT into target (match key = region)
print(f"[4] UPSERT → {SCHEMA}.{TARGET}")
upsert_sql = (
    f"DELETE FROM {SCHEMA}.{TARGET} USING {SCHEMA}.{STAGE} "
    f"WHERE {SCHEMA}.{TARGET}.region = {SCHEMA}.{STAGE}.region; "
    f"INSERT INTO {SCHEMA}.{TARGET} SELECT * FROM {SCHEMA}.{STAGE};"
)

java_import(sc._jvm, "java.sql.*")
conn = sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
stmt = conn.createStatement()
stmt.execute(upsert_sql)
stmt.close()
conn.close()

print("[5] Done — expect 7 rows in target_sales_by_region")
print("=" * 60)
sc.stop()
