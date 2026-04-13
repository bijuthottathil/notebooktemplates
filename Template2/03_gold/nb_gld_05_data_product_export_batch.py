# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_05_data_product_export_batch
# MAGIC **Layer:** Gold | **Pattern:** Data Product Export
# MAGIC **Purpose:** Package and export a certified gold table to an external path for downstream
# MAGIC consumers (Power BI file drop, legacy systems, partner data shares).

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
PRODUCT_NAME  = "sales_summary"
SOURCE_TABLE  = "gold.rpt_sales_monthly"        # schema.table (no catalog prefix)
EXPORT_BASE   = "abfss://datalake@dlscompanydev.dfs.core.windows.net/gold/exports/"
EXPORT_FORMAT = "parquet"                        # parquet | csv | json | delta
FILTER_EXPR   = ""                               # Optional Spark SQL filter; blank = no filter
BATCH_DATE    = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F

RUN_ID      = str(uuid.uuid4())
EXPORT_PATH = f"{EXPORT_BASE.rstrip('/')}/{PRODUCT_NAME}/{BATCH_DATE}/"
DP_TABLE    = f"`{CATALOG}`.`gold`.`dp_{PRODUCT_NAME}`"

print(f"Source      : {CATALOG}.{SOURCE_TABLE}")
print(f"Export Path : {EXPORT_PATH}")
print(f"Format      : {EXPORT_FORMAT}")

# COMMAND ----------
# MAGIC %md ## 1. Read Source Gold Table

# COMMAND ----------

src_df = spark.table(f"{CATALOG}.{SOURCE_TABLE}")

if FILTER_EXPR:
    src_df = src_df.filter(FILTER_EXPR)

# Drop pipeline-internal metadata columns before exposing to consumers
internal_cols = [c for c in src_df.columns if c.startswith("_")]
export_df     = src_df.drop(*internal_cols)

rows_to_export = export_df.count()
print(f"Rows to export : {rows_to_export:,}")
print(f"Columns        : {export_df.columns}")

# COMMAND ----------
# MAGIC %md ## 2. Gate Check (abort if empty)

# COMMAND ----------

if rows_to_export == 0:
    raise RuntimeError(f"Export aborted: 0 rows from '{SOURCE_TABLE}' with filter: '{FILTER_EXPR}'")

# Warn on nulls in key business columns
KEY_COLS = [c for c in ["order_year", "order_month", "account_tier", "total_net_revenue"] if c in export_df.columns]
for c in KEY_COLS:
    null_cnt = export_df.filter(F.col(c).isNull()).count()
    if null_cnt > 0:
        print(f"  WARNING: {null_cnt:,} nulls in '{c}'")

# COMMAND ----------
# MAGIC %md ## 3. Materialise as Certified Data Product Table

# COMMAND ----------

certified_df = (export_df
    .withColumn("dp_product_name",  F.lit(PRODUCT_NAME))
    .withColumn("dp_export_date",   F.lit(BATCH_DATE).cast("date"))
    .withColumn("dp_certified_by",  F.lit("data-platform-team"))
    .withColumn("dp_export_ts",     F.current_timestamp())
    .withColumn("dp_version",       F.lit("1.0"))
)

(certified_df.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"dp_export_date = '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("dp_export_date")
    .saveAsTable(f"{CATALOG}.gold.dp_{PRODUCT_NAME}")
)
print(f"Data product table written: {DP_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Export to External Path

# COMMAND ----------

if EXPORT_FORMAT == "parquet":
    export_df.coalesce(4).write.mode("overwrite").option("compression", "snappy").parquet(EXPORT_PATH)
elif EXPORT_FORMAT == "csv":
    export_df.coalesce(1).write.mode("overwrite").option("header","true").option("compression","gzip").csv(EXPORT_PATH)
elif EXPORT_FORMAT == "json":
    export_df.coalesce(4).write.mode("overwrite").option("compression","gzip").json(EXPORT_PATH)
elif EXPORT_FORMAT == "delta":
    export_df.write.format("delta").mode("overwrite").save(EXPORT_PATH)
else:
    raise ValueError(f"Unsupported export format: {EXPORT_FORMAT}")

print(f"Exported {rows_to_export:,} rows as {EXPORT_FORMAT} → {EXPORT_PATH}")

# COMMAND ----------
# MAGIC %md ## 5. Write Export Manifest

# COMMAND ----------

manifest = spark.createDataFrame([{
    "product_name":  PRODUCT_NAME,
    "export_date":   BATCH_DATE,
    "export_format": EXPORT_FORMAT,
    "export_path":   EXPORT_PATH,
    "row_count":     rows_to_export,
    "run_id":        RUN_ID,
    "exported_at":   str(datetime.datetime.utcnow()),
    "status":        "SUCCESS",
}])

manifest_path = f"{EXPORT_BASE.rstrip('/')}/{PRODUCT_NAME}/_manifest/"
manifest.write.format("delta").mode("append").save(manifest_path)
print(f"Manifest updated: {manifest_path}")
