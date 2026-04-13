# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_03_merge_upsert_batch
# MAGIC **Layer:** Silver
# MAGIC **Pattern:** Merge / Upsert (MERGE INTO)
# MAGIC **Purpose:** Upsert cleansed bronze records into silver using Delta MERGE. Handles late-arriving
# MAGIC data, idempotent reruns, and in-place updates without SCD2 history overhead.
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Transactional tables (orders, events) where only the latest state matters
# MAGIC - Source system provides corrections/amendments to past records
# MAGIC - Replacing Alteryx Join + Update workflows
# MAGIC - Need for idempotent, safe reruns without duplicating data

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",           "company_dev",   "Catalog")
dbutils.widgets.text("source_system",     "erp",           "Source System")
dbutils.widgets.text("entity_name",       "orders",        "Entity Name")
dbutils.widgets.text("domain",            "supply_chain",  "Silver Domain")
dbutils.widgets.text("merge_key_cols",    "order_id",      "Merge Key Columns (comma-separated)")
dbutils.widgets.text("update_cols",       "",              "Columns to Update on Match (blank = all non-key cols)")
dbutils.widgets.text("batch_date",        "",              "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("late_arrival_days", "7",             "Accept late arrivals up to N days back")
dbutils.widgets.dropdown("env",           "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG       = dbutils.widgets.get("catalog")
SOURCE_SYS    = dbutils.widgets.get("source_system")
ENTITY_NAME   = dbutils.widgets.get("entity_name")
DOMAIN        = dbutils.widgets.get("domain")
MERGE_KEYS    = [k.strip() for k in dbutils.widgets.get("merge_key_cols").split(",")]
UPDATE_COLS   = [c.strip() for c in dbutils.widgets.get("update_cols").split(",") if c.strip()]
LATE_DAYS     = int(dbutils.widgets.get("late_arrival_days"))

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID        = str(uuid.uuid4())

SRC_TABLE     = f"`{CATALOG}`.`bronze`.`{SOURCE_SYS}__{ENTITY_NAME}`"
TGT_TABLE     = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}`"

late_cutoff   = str(datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=LATE_DAYS))

print(f"Source     : {SRC_TABLE}")
print(f"Target     : {TGT_TABLE}")
print(f"Merge Keys : {MERGE_KEYS}")
print(f"Late Cutoff: {late_cutoff}")

# COMMAND ----------
# MAGIC %md ## 2. Read Bronze — Batch Window + Late Arrival

# COMMAND ----------

incoming_df = (spark.table(SRC_TABLE)
    .filter(F.col("_ingest_date").between(
        F.lit(late_cutoff).cast("date"),
        F.lit(BATCH_DATE).cast("date")
    ))
)
rows_incoming = incoming_df.count()
print(f"Incoming rows (incl. late arrivals from {late_cutoff}): {rows_incoming:,}")

# Drop bronze internal metadata cols
meta_cols   = [c for c in incoming_df.columns if c.startswith("_")]
data_df     = incoming_df.drop(*meta_cols)

# Add silver metadata
staging_df = (data_df
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",     F.lit(SOURCE_SYS))
    .withColumn("_source_entity",     F.lit(ENTITY_NAME))
)

staging_df.createOrReplaceTempView("_merge_source")

# COMMAND ----------
# MAGIC %md ## 3. Ensure Target Table Exists

# COMMAND ----------

# Auto-create target if first run
table_exists = spark.catalog.tableExists(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME.replace('`', '')}")

if not table_exists:
    (staging_df.limit(0).write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_silver_batch_date")
        .saveAsTable(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
    )
    print(f"Created target table: {TGT_TABLE}")
else:
    print(f"Target table exists: {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Build MERGE Statement

# COMMAND ----------

join_condition = " AND ".join([f"target.{k} = source.{k}" for k in MERGE_KEYS])

# Determine columns to update
if UPDATE_COLS:
    update_set = ", ".join([f"target.{c} = source.{c}" for c in UPDATE_COLS])
else:
    all_cols    = [c for c in staging_df.columns if c not in MERGE_KEYS]
    update_set  = ", ".join([f"target.{c} = source.{c}" for c in all_cols])

insert_cols = ", ".join(staging_df.columns)
insert_vals = ", ".join([f"source.{c}" for c in staging_df.columns])

merge_sql = f"""
    MERGE INTO {TGT_TABLE} AS target
    USING _merge_source AS source
    ON {join_condition}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
"""

print("MERGE SQL:")
print(merge_sql)

# COMMAND ----------
# MAGIC %md ## 5. Execute MERGE

# COMMAND ----------

merge_result = spark.sql(merge_sql)
merge_metrics = merge_result.collect()[0].asDict() if merge_result.columns else {}
print(f"MERGE complete: {merge_metrics}")

# COMMAND ----------
# MAGIC %md ## 6. Verify Idempotency (Optional — dev/staging only)

# COMMAND ----------

# Re-running MERGE with same source should produce 0 inserts, 0 updates
# Uncomment in non-prod to verify:
# merge_result2 = spark.sql(merge_sql)
# print("Idempotency check:", merge_result2.collect())

# COMMAND ----------
# MAGIC %md ## 7. Optimize Target

# COMMAND ----------

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY ({', '.join(MERGE_KEYS)})")
print(f"OPTIMIZE applied on {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 8. Row Count

# COMMAND ----------

rows_in_target = spark.table(TGT_TABLE).count()
print(f"Total rows in {TGT_TABLE}: {rows_in_target:,}")

dbutils.jobs.taskValues.set(key="silver_table",   value=f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
dbutils.jobs.taskValues.set(key="rows_in_target", value=rows_in_target)
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_in_target}")
