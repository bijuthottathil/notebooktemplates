# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_01_full_load_batch
# MAGIC **Layer:** Bronze
# MAGIC **Pattern:** Full Batch Load (Truncate & Reload)
# MAGIC **Purpose:** Ingest complete dataset from landing zone — replaces entire target on every run.
# MAGIC Use when source system always exports a full snapshot (common in Alteryx migrations).
# MAGIC
# MAGIC **Supported Formats:** CSV · Excel (xlsx/xls) · JSON · Parquet · ORC
# MAGIC
# MAGIC **Naming Convention** *(inspired by Uber Hudi / Netflix conventions)*:
# MAGIC ```
# MAGIC Notebook : nb_<layer>_<seq>_<pattern>
# MAGIC Table     : <catalog>.<layer>.<source>__<entity>   (dbt-style double underscore = source boundary)
# MAGIC Partition : ingest_date
# MAGIC ```
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Source always delivers a complete file (no delta/CDC available)
# MAGIC - Small-to-medium tables (< 500M rows) where full reload is acceptable
# MAGIC - Replacing Alteryx workflows that read a file and write to a target without watermarking

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",          "company_dev",         "Catalog")
dbutils.widgets.text("source_system",    "crm",                 "Source System")
dbutils.widgets.text("entity_name",      "customers",           "Entity / Table Name")
dbutils.widgets.text("source_path",      "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/crm/customers/", "Source Path (ADLS)")
dbutils.widgets.dropdown("file_format",  "csv", ["csv", "excel", "json", "parquet", "orc"], "File Format")
dbutils.widgets.text("file_pattern",     "*.csv",               "File Pattern (glob)")
dbutils.widgets.text("batch_date",       "",                    "Batch Date (YYYY-MM-DD, blank=today)")
dbutils.widgets.dropdown("env",          "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid

CATALOG       = dbutils.widgets.get("catalog")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")
ENTITY_NAME   = dbutils.widgets.get("entity_name")
SOURCE_PATH   = dbutils.widgets.get("source_path")
FILE_FORMAT   = dbutils.widgets.get("file_format")
FILE_PATTERN  = dbutils.widgets.get("file_pattern")
ENV           = dbutils.widgets.get("env")

raw_batch_date = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_batch_date if raw_batch_date else str(datetime.date.today())
RUN_ID        = str(uuid.uuid4())
TARGET_SCHEMA = "bronze"
TARGET_TABLE  = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"
FULL_TABLE    = f"`{CATALOG}`.`{TARGET_SCHEMA}`.`{TARGET_TABLE}`"

print(f"Run ID        : {RUN_ID}")
print(f"Target Table  : {FULL_TABLE}")
print(f"Batch Date    : {BATCH_DATE}")
print(f"Source Path   : {SOURCE_PATH}")
print(f"File Format   : {FILE_FORMAT}")

# COMMAND ----------
# MAGIC %md ## 2. Audit — Log Run Start

# COMMAND ----------

import datetime as dt
from pyspark.sql import functions as F

def log_pipeline_start(run_id, pipeline_name, layer, source_system, entity_name, batch_date, load_type):
    spark.sql(f"""
        INSERT INTO `{CATALOG}`.`audit`.`pipeline_run_log`
        (run_id, pipeline_name, layer, source_system, entity_name, batch_date, load_type,
         status, rows_read, rows_written, rows_rejected, start_ts, end_ts, duration_seconds, error_message, notebook_path)
        VALUES ('{run_id}', '{pipeline_name}', '{layer}', '{source_system}', '{entity_name}',
                '{batch_date}', '{load_type}', 'RUNNING', 0, 0, 0,
                current_timestamp(), NULL, NULL, NULL,
                '{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}')
    """)

def log_pipeline_end(run_id, status, rows_read, rows_written, rows_rejected, start_ts, error_msg=""):
    elapsed = (dt.datetime.utcnow() - start_ts).total_seconds()
    safe_error = error_msg.replace("'", "''")[:2000]
    spark.sql(f"""
        UPDATE `{CATALOG}`.`audit`.`pipeline_run_log`
        SET status = '{status}', rows_read = {rows_read}, rows_written = {rows_written},
            rows_rejected = {rows_rejected}, end_ts = current_timestamp(),
            duration_seconds = {elapsed:.2f}, error_message = '{safe_error}'
        WHERE run_id = '{run_id}'
    """)

START_TS = dt.datetime.utcnow()
NOTEBOOK_NAME = "nb_brz_01_full_load_batch"
log_pipeline_start(RUN_ID, NOTEBOOK_NAME, TARGET_SCHEMA, SOURCE_SYSTEM, ENTITY_NAME, BATCH_DATE, "full")
print(f"Audit log started — run_id: {RUN_ID}")

# COMMAND ----------
# MAGIC %md ## 3. Read Source File(s)

# COMMAND ----------

def read_source(path: str, fmt: str, pattern: str):
    """Auto-dispatch reader by format. Returns a Spark DataFrame."""
    full_path = path.rstrip("/") + "/" + pattern

    if fmt == "csv":
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .option("multiLine", "true")
              .option("escape", '"')
              .csv(full_path))

    elif fmt == "excel":
        # Requires com.crealytics:spark-excel library on cluster
        df = (spark.read
              .format("com.crealytics.spark.excel")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("dataAddress", "'Sheet1'!A1")
              .load(full_path))

    elif fmt == "json":
        df = (spark.read
              .option("multiLine", "true")
              .json(full_path))

    elif fmt == "parquet":
        df = spark.read.parquet(full_path)

    elif fmt == "orc":
        df = spark.read.orc(full_path)

    else:
        raise ValueError(f"Unsupported file format: {fmt}")

    return df

raw_df = read_source(SOURCE_PATH, FILE_FORMAT, FILE_PATTERN)
rows_read = raw_df.count()
print(f"Rows read: {rows_read:,}")
print(f"Columns : {raw_df.columns}")

# COMMAND ----------
# MAGIC %md ## 4. Add Bronze Metadata Columns

# COMMAND ----------

# Standardise column names to snake_case
import re

def to_snake_case(col_name: str) -> str:
    s = re.sub(r"[\s\-\.]+", "_", col_name.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().lstrip("_")

renamed_df = raw_df
for col in raw_df.columns:
    snake = to_snake_case(col)
    if snake != col:
        renamed_df = renamed_df.withColumnRenamed(col, snake)

# Add bronze metadata
bronze_df = (renamed_df
    .withColumn("_ingest_id",        F.lit(RUN_ID))
    .withColumn("_ingest_ts",        F.current_timestamp())
    .withColumn("_ingest_date",      F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",    F.lit(SOURCE_SYSTEM))
    .withColumn("_source_entity",    F.lit(ENTITY_NAME))
    .withColumn("_source_file_path", F.input_file_name())
    .withColumn("_source_format",    F.lit(FILE_FORMAT))
    .withColumn("_batch_date",       F.lit(BATCH_DATE).cast("date"))
)

print(f"Schema after metadata enrichment ({len(bronze_df.columns)} columns):")
bronze_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 5. Write to Bronze Delta Table (Overwrite)

# COMMAND ----------

(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_ingest_date")
    .option("userMetadata", f"run_id={RUN_ID},batch={BATCH_DATE}")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)

rows_written = spark.table(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}").count()
print(f"Rows written to {FULL_TABLE}: {rows_written:,}")

# COMMAND ----------
# MAGIC %md ## 6. Apply Delta Optimizations

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULL_TABLE} ZORDER BY (_ingest_date)")
print(f"OPTIMIZE + ZORDER applied on {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 7. Audit — Log Run End

# COMMAND ----------

log_pipeline_end(RUN_ID, "SUCCESS", rows_read, rows_written, 0, START_TS)
print(f"Pipeline complete — {rows_written:,} rows in {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 8. Output for Downstream Tasks

# COMMAND ----------

dbutils.jobs.taskValues.set(key="bronze_table",  value=f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
dbutils.jobs.taskValues.set(key="rows_written",  value=rows_written)
dbutils.jobs.taskValues.set(key="run_id",        value=RUN_ID)
dbutils.jobs.taskValues.set(key="batch_date",    value=BATCH_DATE)

dbutils.notebook.exit(f"SUCCESS|{rows_written}")
