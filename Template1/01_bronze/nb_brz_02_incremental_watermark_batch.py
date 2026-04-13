# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_02_incremental_watermark_batch
# MAGIC **Layer:** Bronze
# MAGIC **Pattern:** Incremental Load with Watermark
# MAGIC **Purpose:** Ingest only new/changed records since the last successful run using a high-watermark
# MAGIC column (e.g., `updated_at`, `created_date`, `record_seq`). Appends to the bronze Delta table.
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Source exports daily/hourly delta files named with a timestamp or date suffix
# MAGIC - Large tables where full reload is cost-prohibitive
# MAGIC - Source has a reliable `updated_at` or sequence column
# MAGIC - Replacing Alteryx incremental workflows
# MAGIC
# MAGIC **Watermark Storage:** Last processed watermark is persisted in `audit.watermark_control` table.

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",           "company_dev",        "Catalog")
dbutils.widgets.text("source_system",     "erp",                "Source System")
dbutils.widgets.text("entity_name",       "orders",             "Entity / Table Name")
dbutils.widgets.text("source_path",       "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/erp/orders/", "Source Path (ADLS)")
dbutils.widgets.dropdown("file_format",   "parquet", ["csv", "excel", "json", "parquet", "orc"], "File Format")
dbutils.widgets.text("file_pattern",      "*.parquet",          "File Pattern")
dbutils.widgets.text("watermark_col",     "updated_at",         "Watermark Column Name")
dbutils.widgets.text("batch_date",        "",                   "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env",           "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid, re
from pyspark.sql import functions as F

CATALOG         = dbutils.widgets.get("catalog")
SOURCE_SYSTEM   = dbutils.widgets.get("source_system")
ENTITY_NAME     = dbutils.widgets.get("entity_name")
SOURCE_PATH     = dbutils.widgets.get("source_path")
FILE_FORMAT     = dbutils.widgets.get("file_format")
FILE_PATTERN    = dbutils.widgets.get("file_pattern")
WATERMARK_COL   = dbutils.widgets.get("watermark_col")

raw_bd          = dbutils.widgets.get("batch_date").strip()
BATCH_DATE      = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID          = str(uuid.uuid4())
TARGET_SCHEMA   = "bronze"
TARGET_TABLE    = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"
FULL_TABLE      = f"`{CATALOG}`.`{TARGET_SCHEMA}`.`{TARGET_TABLE}`"
WM_TABLE        = f"`{CATALOG}`.`audit`.`watermark_control`"

print(f"Run ID       : {RUN_ID}")
print(f"Target Table : {FULL_TABLE}")
print(f"Watermark Col: {WATERMARK_COL}")
print(f"Batch Date   : {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 2. Create Watermark Control Table (if not exists)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WM_TABLE} (
        source_system   STRING,
        entity_name     STRING,
        watermark_col   STRING,
        last_watermark  STRING    COMMENT 'Stored as string; cast at read time',
        last_run_id     STRING,
        last_updated    TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Tracks high-watermark per source entity for incremental loads'
""")
print(f"Watermark control table ready: {WM_TABLE}")

# COMMAND ----------
# MAGIC %md ## 3. Read Current Watermark

# COMMAND ----------

wm_row = spark.sql(f"""
    SELECT last_watermark
    FROM {WM_TABLE}
    WHERE source_system = '{SOURCE_SYSTEM}' AND entity_name = '{ENTITY_NAME}'
    ORDER BY last_updated DESC LIMIT 1
""").collect()

if wm_row:
    last_watermark = wm_row[0]["last_watermark"]
    print(f"Resuming from watermark: {last_watermark}")
else:
    last_watermark = "1900-01-01 00:00:00"
    print(f"No prior watermark found — using epoch: {last_watermark}")

# COMMAND ----------
# MAGIC %md ## 4. Read Source Files

# COMMAND ----------

def read_source(path, fmt, pattern):
    full_path = path.rstrip("/") + "/" + pattern
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(full_path)
    elif fmt == "excel":
        return (spark.read.format("com.crealytics.spark.excel")
                .option("header", "true").option("inferSchema", "true").load(full_path))
    elif fmt == "json":
        return spark.read.option("multiLine", "true").json(full_path)
    elif fmt == "parquet":
        return spark.read.parquet(full_path)
    elif fmt == "orc":
        return spark.read.orc(full_path)
    raise ValueError(f"Unsupported format: {fmt}")

all_df = read_source(SOURCE_PATH, FILE_FORMAT, FILE_PATTERN)
total_rows_in_files = all_df.count()
print(f"Total rows in landing files: {total_rows_in_files:,}")

# COMMAND ----------
# MAGIC %md ## 5. Apply Watermark Filter

# COMMAND ----------

incremental_df = all_df.filter(F.col(WATERMARK_COL) > F.lit(last_watermark).cast("timestamp"))
rows_read = incremental_df.count()
print(f"Rows after watermark filter ({WATERMARK_COL} > {last_watermark}): {rows_read:,}")

if rows_read == 0:
    print("No new records to process. Exiting.")
    dbutils.notebook.exit("NO_NEW_DATA|0")

# COMMAND ----------
# MAGIC %md ## 6. Add Bronze Metadata Columns

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

wm_snake = to_snake_case(WATERMARK_COL)

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
# MAGIC %md ## 7. Append to Bronze Delta Table

# COMMAND ----------

(bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)

rows_written = bronze_df.count()
print(f"Appended {rows_written:,} rows to {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 8. Update Watermark

# COMMAND ----------

# Compute new max watermark from this batch
new_wm_row = bronze_df.agg(F.max(F.col(wm_snake)).cast("string").alias("max_wm")).collect()
new_watermark = new_wm_row[0]["max_wm"] if new_wm_row else last_watermark

spark.sql(f"""
    MERGE INTO {WM_TABLE} AS target
    USING (SELECT
        '{SOURCE_SYSTEM}'  AS source_system,
        '{ENTITY_NAME}'    AS entity_name,
        '{WATERMARK_COL}'  AS watermark_col,
        '{new_watermark}'  AS last_watermark,
        '{RUN_ID}'         AS last_run_id
    ) AS source
    ON  target.source_system = source.source_system
    AND target.entity_name   = source.entity_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"Watermark updated: {last_watermark}  →  {new_watermark}")

# COMMAND ----------
# MAGIC %md ## 9. Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULL_TABLE} ZORDER BY (_ingest_date, {wm_snake})")

# COMMAND ----------
# MAGIC %md ## 10. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="bronze_table",   value=f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
dbutils.jobs.taskValues.set(key="rows_written",   value=rows_written)
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)
dbutils.jobs.taskValues.set(key="new_watermark",  value=new_watermark)

dbutils.notebook.exit(f"SUCCESS|{rows_written}")
