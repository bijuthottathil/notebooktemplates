# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_02_incremental_watermark_batch
# MAGIC **Layer:** Bronze | **Pattern:** Incremental Load with Watermark
# MAGIC **Purpose:** Append only new/changed records since the last successful run.
# MAGIC Watermark is stored in audit.watermark_control and updated after each run.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYSTEM = "erp"
ENTITY_NAME   = "orders"
SOURCE_PATH   = "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/erp/orders/"
FILE_FORMAT   = "parquet"    # csv | excel | json | parquet | orc
FILE_PATTERN  = "*.parquet"
WATERMARK_COL = "updated_at" # Column used to filter new records
BATCH_DATE    = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid, re
from pyspark.sql import functions as F

RUN_ID        = str(uuid.uuid4())
TARGET_SCHEMA = "bronze"
TARGET_TABLE  = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"
FULL_TABLE    = f"`{CATALOG}`.`{TARGET_SCHEMA}`.`{TARGET_TABLE}`"
WM_TABLE      = f"`{CATALOG}`.`audit`.`watermark_control`"

print(f"Run ID        : {RUN_ID}")
print(f"Target Table  : {FULL_TABLE}")
print(f"Watermark Col : {WATERMARK_COL}")

# COMMAND ----------
# MAGIC %md ## 1. Read Current Watermark

# COMMAND ----------

wm_row = spark.sql(f"""
    SELECT last_watermark FROM {WM_TABLE}
    WHERE source_system = '{SOURCE_SYSTEM}' AND entity_name = '{ENTITY_NAME}'
    ORDER BY last_updated DESC LIMIT 1
""").collect()

last_watermark = wm_row[0]["last_watermark"] if wm_row else "1900-01-01 00:00:00"
print(f"Last watermark: {last_watermark}")

# COMMAND ----------
# MAGIC %md ## 2. Read Source Files

# COMMAND ----------

full_path = SOURCE_PATH.rstrip("/") + "/" + FILE_PATTERN

if FILE_FORMAT == "csv":
    all_df = spark.read.option("header", "true").option("inferSchema", "true").csv(full_path)
elif FILE_FORMAT == "excel":
    all_df = (spark.read.format("com.crealytics.spark.excel")
              .option("header", "true").option("inferSchema", "true").load(full_path))
elif FILE_FORMAT == "json":
    all_df = spark.read.option("multiLine", "true").json(full_path)
elif FILE_FORMAT == "parquet":
    all_df = spark.read.parquet(full_path)
elif FILE_FORMAT == "orc":
    all_df = spark.read.orc(full_path)
else:
    raise ValueError(f"Unsupported format: {FILE_FORMAT}")

incremental_df = all_df.filter(F.col(WATERMARK_COL) > F.lit(last_watermark).cast("timestamp"))
rows_read      = incremental_df.count()
print(f"Rows after watermark filter: {rows_read:,}")

if rows_read == 0:
    print("No new records. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA|0")

# COMMAND ----------
# MAGIC %md ## 3. Normalise Columns & Add Metadata

# COMMAND ----------

def to_snake_case(col_name):
    s = re.sub(r"[\s\-\.]+", "_", col_name.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().lstrip("_")

renamed_df = incremental_df
for col in incremental_df.columns:
    snake = to_snake_case(col)
    if snake != col:
        renamed_df = renamed_df.withColumnRenamed(col, snake)

wm_snake  = to_snake_case(WATERMARK_COL)
bronze_df = (renamed_df
    .withColumn("_ingest_id",        F.lit(RUN_ID))
    .withColumn("_ingest_ts",        F.current_timestamp())
    .withColumn("_ingest_date",      F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",    F.lit(SOURCE_SYSTEM))
    .withColumn("_source_entity",    F.lit(ENTITY_NAME))
    .withColumn("_source_file_path", F.input_file_name())
    .withColumn("_load_type",        F.lit("incremental"))
    .withColumn("_batch_date",       F.lit(BATCH_DATE).cast("date"))
)

# COMMAND ----------
# MAGIC %md ## 4. Append to Bronze

# COMMAND ----------

(bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)

rows_written  = bronze_df.count()
new_wm        = bronze_df.agg(F.max(F.col(wm_snake)).cast("string")).collect()[0][0]
print(f"Appended {rows_written:,} rows. New watermark: {new_wm}")

# COMMAND ----------
# MAGIC %md ## 5. Update Watermark

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {WM_TABLE} AS target
    USING (SELECT '{SOURCE_SYSTEM}' AS source_system, '{ENTITY_NAME}' AS entity_name,
                  '{WATERMARK_COL}' AS watermark_col, '{new_wm}' AS last_watermark,
                  '{RUN_ID}' AS last_run_id) AS source
    ON  target.source_system = source.source_system
    AND target.entity_name   = source.entity_name
    WHEN MATCHED     THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"Watermark updated: {last_watermark} → {new_wm}")

# COMMAND ----------
# MAGIC %md ## 6. Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULL_TABLE} ZORDER BY (_ingest_date, {wm_snake})")
print(f"Pipeline complete — {rows_written:,} rows appended.")
