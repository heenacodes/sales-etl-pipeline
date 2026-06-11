"""
Glue Job 3: Aggregate by country + item
  target_sales → group by country, item_type → stage → UPSERT target

Expected result: 736 rows
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
STAGE = "stage_sales_by_country_item"
TARGET = "target_sales_by_country_item"

print("=" * 60)
print("JOB 3: Aggregate by country + item")
print("=" * 60)

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
    print("[WARN] No data in target_sales")
    sc.stop()
    sys.exit(0)

print("[2] Grouping by country + item_type")
df_agg = df.groupBy("country", "item_type").agg(
    spark_sum("total_revenue").alias("total_revenue"),
    spark_sum("total_cost").alias("total_cost"),
    spark_sum("total_profit").alias("total_profit"),
    spark_sum("units_sold").alias("total_units_sold"),
    count("*").alias("order_count"),
)
print(f"    Groups: {df_agg.count()}")

print(f"[3] Writing {SCHEMA}.{STAGE}")
df_agg.write.format("jdbc").option("url", jdbc_opts["url"]).option(
    "dbtable", f"{SCHEMA}.{STAGE}"
).option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option(
    "driver", jdbc_opts["driver"]
).option("truncate", "true").mode("overwrite").save()

print(f"[4] UPSERT → {SCHEMA}.{TARGET}")
upsert_sql = (
    f"DELETE FROM {SCHEMA}.{TARGET} USING {SCHEMA}.{STAGE} "
    f"WHERE {SCHEMA}.{TARGET}.country = {SCHEMA}.{STAGE}.country "
    f"AND {SCHEMA}.{TARGET}.item_type = {SCHEMA}.{STAGE}.item_type; "
    f"INSERT INTO {SCHEMA}.{TARGET} SELECT * FROM {SCHEMA}.{STAGE};"
)

java_import(sc._jvm, "java.sql.*")
conn = sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
stmt = conn.createStatement()
stmt.execute(upsert_sql)
stmt.close()
conn.close()

print("[5] Done — expect 736 rows")
print("=" * 60)
sc.stop()
