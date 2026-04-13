# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_01_full_load_batch
# MAGIC **Layer:** Bronze | **Pattern:** Full Batch Load (Truncate & Reload)
# MAGIC **Purpose:** Ingest a complete file snapshot from landing zone into bronze Delta table.
# MAGIC Replaces the entire target on every run.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYSTEM = "crm"
ENTITY_NAME   = "customers"
SOURCE_PATH   = "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/crm/customers/"
FILE_FORMAT   = "csv"        # csv | excel | json | parquet | orc
FILE_PATTERN  = "*.csv"
BATCH_DATE    = str(__import__("datetime").date.today())   # override: "2024-04-01"

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid, re
from pyspark.sql import functions as F

RUN_ID        = str(uuid.uuid4())
TARGET_SCHEMA = "bronze"
TARGET_TABLE  = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"
FULL_TABLE    = f"`{CATALOG}`.`{TARGET_SCHEMA}`.`{TARGET_TABLE}`"

print(f"Run ID       : {RUN_ID}")
print(f"Target Table : {FULL_TABLE}")
print(f"Batch Date   : {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 1. Log Pipeline Start

# COMMAND ----------

START_TS = datetime.datetime.utcnow()

spark.sql(f"""
    INSERT INTO `{CATALOG}`.`audit`.`pipeline_run_log`
    (run_id, pipeline_name, layer, source_system, entity_name, batch_date, load_type,
     status, rows_read, rows_written, rows_rejected, start_ts, end_ts, duration_seconds, error_message, notebook_path)
    VALUES ('{RUN_ID}', 'nb_brz_01_full_load_batch', '{TARGET_SCHEMA}', '{SOURCE_SYSTEM}', '{ENTITY_NAME}',
            '{BATCH_DATE}', 'full', 'RUNNING', 0, 0, 0, current_timestamp(), NULL, NULL, NULL, 'nb_brz_01_full_load_batch')
""")

# COMMAND ----------
# MAGIC %md ## 2. Read Source File(s)

# COMMAND ----------

full_path = SOURCE_PATH.rstrip("/") + "/" + FILE_PATTERN

if FILE_FORMAT == "csv":
    raw_df = (spark.read
              .option("header", "true").option("inferSchema", "true")
              .option("multiLine", "true").option("escape", '"')
              .csv(full_path))

elif FILE_FORMAT == "excel":
    raw_df = (spark.read
              .format("com.crealytics.spark.excel")
              .option("header", "true").option("inferSchema", "true")
              .option("dataAddress", "'Sheet1'!A1")
              .load(full_path))

elif FILE_FORMAT == "json":
    raw_df = spark.read.option("multiLine", "true").json(full_path)

elif FILE_FORMAT == "parquet":
    raw_df = spark.read.parquet(full_path)

elif FILE_FORMAT == "orc":
    raw_df = spark.read.orc(full_path)

else:
    raise ValueError(f"Unsupported format: {FILE_FORMAT}")

rows_read = raw_df.count()
print(f"Rows read : {rows_read:,}")
print(f"Columns   : {raw_df.columns}")

# COMMAND ----------
# MAGIC %md ## 3. Normalise Column Names & Add Metadata

# COMMAND ----------

def to_snake_case(col_name):
    s = re.sub(r"[\s\-\.]+", "_", col_name.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().lstrip("_")

renamed_df = raw_df
for col in raw_df.columns:
    snake = to_snake_case(col)
    if snake != col:
        renamed_df = renamed_df.withColumnRenamed(col, snake)

bronze_df = (renamed_df
    .withColumn("_ingest_id",        F.lit(RUN_ID))
    .withColumn("_ingest_ts",        F.current_timestamp())
    .withColumn("_ingest_date",      F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",    F.lit(SOURCE_SYSTEM))
    .withColumn("_source_entity",    F.lit(ENTITY_NAME))
    .withColumn("_source_file_path", F.input_file_name())
    .withColumn("_source_format",    F.lit(FILE_FORMAT))
    .withColumn("_load_type",        F.lit("full"))
    .withColumn("_batch_date",       F.lit(BATCH_DATE).cast("date"))
)

# COMMAND ----------
# MAGIC %md ## 4. Write to Bronze (Overwrite)

# COMMAND ----------

(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)

rows_written = spark.table(FULL_TABLE).count()
print(f"Rows written : {rows_written:,} → {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Optimize & Log End

# COMMAND ----------

spark.sql(f"OPTIMIZE {FULL_TABLE} ZORDER BY (_ingest_date)")

elapsed = (datetime.datetime.utcnow() - START_TS).total_seconds()
spark.sql(f"""
    UPDATE `{CATALOG}`.`audit`.`pipeline_run_log`
    SET status = 'SUCCESS', rows_read = {rows_read}, rows_written = {rows_written},
        end_ts = current_timestamp(), duration_seconds = {elapsed:.2f}
    WHERE run_id = '{RUN_ID}'
""")

print(f"Pipeline complete in {elapsed:.1f}s — {rows_written:,} rows in {FULL_TABLE}")
