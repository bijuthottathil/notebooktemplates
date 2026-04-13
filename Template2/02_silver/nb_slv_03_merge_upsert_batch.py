# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_03_merge_upsert_batch
# MAGIC **Layer:** Silver | **Pattern:** Merge / Upsert (MERGE INTO)
# MAGIC **Purpose:** Upsert bronze records into silver. Updates existing rows and inserts new ones.
# MAGIC Handles late-arriving data within a configurable lookback window. Idempotent on reruns.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYS    = "erp"
ENTITY_NAME   = "orders"
DOMAIN        = "supply_chain"
MERGE_KEYS    = ["order_id"]          # Column(s) used to match source to target
UPDATE_COLS   = []                    # Specific columns to update; leave empty to update all
LATE_DAYS     = 7                     # Accept late arrivals up to N days back
BATCH_DATE    = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F

RUN_ID      = str(uuid.uuid4())
SRC_TABLE   = f"`{CATALOG}`.`bronze`.`{SOURCE_SYS}__{ENTITY_NAME}`"
TGT_TABLE   = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}`"
LATE_CUTOFF = str(datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=LATE_DAYS))

print(f"Source     : {SRC_TABLE}")
print(f"Target     : {TGT_TABLE}")
print(f"Merge Keys : {MERGE_KEYS}")
print(f"Late Cutoff: {LATE_CUTOFF}")

# COMMAND ----------
# MAGIC %md ## 1. Read Bronze (Batch Window + Late Arrivals)

# COMMAND ----------

incoming_df = spark.table(SRC_TABLE).filter(
    F.col("_ingest_date").between(
        F.lit(LATE_CUTOFF).cast("date"),
        F.lit(BATCH_DATE).cast("date")
    )
)
rows_incoming = incoming_df.count()
print(f"Incoming rows (incl. late arrivals): {rows_incoming:,}")

meta_cols  = [c for c in incoming_df.columns if c.startswith("_")]
staging_df = (incoming_df.drop(*meta_cols)
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",     F.lit(SOURCE_SYS))
    .withColumn("_source_entity",     F.lit(ENTITY_NAME))
)

staging_df.createOrReplaceTempView("_merge_source")

# COMMAND ----------
# MAGIC %md ## 2. Auto-Create Target Table on First Run

# COMMAND ----------

table_exists = spark.catalog.tableExists(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
if not table_exists:
    (staging_df.limit(0).write
        .format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_silver_batch_date")
        .saveAsTable(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
    )
    print(f"Target table created: {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 3. Build & Execute MERGE

# COMMAND ----------

join_cond  = " AND ".join([f"target.{k} = source.{k}" for k in MERGE_KEYS])
all_cols   = [c for c in staging_df.columns if c not in MERGE_KEYS]
update_set = ", ".join([f"target.{c} = source.{c}" for c in (UPDATE_COLS if UPDATE_COLS else all_cols)])
insert_cols = ", ".join(staging_df.columns)
insert_vals = ", ".join([f"source.{c}" for c in staging_df.columns])

merge_sql = f"""
    MERGE INTO {TGT_TABLE} AS target
    USING _merge_source AS source
    ON {join_cond}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
"""

spark.sql(merge_sql)
print(f"MERGE complete.")

# COMMAND ----------
# MAGIC %md ## 4. Optimize & Row Count

# COMMAND ----------

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY ({', '.join(MERGE_KEYS)})")

rows_in_target = spark.table(TGT_TABLE).count()
print(f"Total rows in {TGT_TABLE}: {rows_in_target:,}")
