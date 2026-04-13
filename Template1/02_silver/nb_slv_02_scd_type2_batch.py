# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_02_scd_type2_batch
# MAGIC **Layer:** Silver
# MAGIC **Pattern:** Slowly Changing Dimension Type 2 (SCD2)
# MAGIC **Purpose:** Maintain full history of dimension record changes. When an attribute changes, the
# MAGIC current record is expired and a new record is inserted. Implements the SCD2 pattern
# MAGIC using Delta Lake MERGE.
# MAGIC
# MAGIC **SCD2 Columns Added:**
# MAGIC | Column              | Description                          |
# MAGIC |---------------------|--------------------------------------|
# MAGIC | `_effective_from`   | Date this version became active      |
# MAGIC | `_effective_to`     | Date this version expired (NULL = current) |
# MAGIC | `_is_current`       | Boolean flag for latest record       |
# MAGIC | `_record_hash`      | MD5 hash of tracked attributes       |
# MAGIC | `_silver_ingest_ts` | Timestamp loaded to silver           |
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Dimension tables where history must be preserved (customers, products, employees)
# MAGIC - Replacing Alteryx Append Field / Join workflows that tracked changes manually
# MAGIC - Any entity where "what was the value on date X?" is a valid business question

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",           "company_dev",      "Catalog")
dbutils.widgets.text("source_system",     "crm",              "Source System")
dbutils.widgets.text("entity_name",       "customers",        "Entity Name")
dbutils.widgets.text("domain",            "crm",              "Silver Domain")
dbutils.widgets.text("natural_key_cols",  "customer_id",      "Natural Key (comma-separated)")
dbutils.widgets.text("tracked_cols",      "customer_name,email,segment,country", "SCD2 Tracked Columns (comma-separated)")
dbutils.widgets.text("batch_date",        "",                 "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env",           "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG      = dbutils.widgets.get("catalog")
SOURCE_SYS   = dbutils.widgets.get("source_system")
ENTITY_NAME  = dbutils.widgets.get("entity_name")
DOMAIN       = dbutils.widgets.get("domain")
NAT_KEYS     = [k.strip() for k in dbutils.widgets.get("natural_key_cols").split(",")]
TRACKED_COLS = [c.strip() for c in dbutils.widgets.get("tracked_cols").split(",")]

raw_bd       = dbutils.widgets.get("batch_date").strip()
BATCH_DATE   = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID       = str(uuid.uuid4())

SRC_TABLE    = f"`{CATALOG}`.`bronze`.`{SOURCE_SYS}__{ENTITY_NAME}`"
TGT_TABLE    = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}_history`"

print(f"Source     : {SRC_TABLE}")
print(f"Target     : {TGT_TABLE}")
print(f"Natural Key: {NAT_KEYS}")
print(f"Tracked    : {TRACKED_COLS}")

# COMMAND ----------
# MAGIC %md ## 2. Create Target SCD2 Table (if not exists)

# COMMAND ----------

# Build key columns DDL
key_col_ddl = "\n".join([f"    {k}  STRING  NOT NULL," for k in NAT_KEYS])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE} (
    {key_col_ddl}
        _effective_from     DATE        NOT NULL  COMMENT 'Date this version became active',
        _effective_to       DATE                  COMMENT 'Date this version expired — NULL means current',
        _is_current         BOOLEAN     NOT NULL  COMMENT 'True for the active record',
        _record_hash        STRING                COMMENT 'MD5 of tracked attribute values',
        _silver_ingest_id   STRING,
        _silver_ingest_ts   TIMESTAMP,
        _source_system      STRING,
        _source_entity      STRING
    )
    USING DELTA
    COMMENT 'SCD Type 2 history table for {ENTITY_NAME}'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f"SCD2 target table ready: {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 3. Read Incoming Batch from Bronze

# COMMAND ----------

incoming_df = (spark.table(SRC_TABLE)
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
)
rows_incoming = incoming_df.count()
print(f"Incoming rows: {rows_incoming:,}")

# Compute hash of tracked columns to detect changes
hash_expr = F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in TRACKED_COLS if c in incoming_df.columns]))

staging_df = (incoming_df
    .withColumn("_record_hash",       hash_expr)
    .withColumn("_effective_from",    F.lit(BATCH_DATE).cast("date"))
    .withColumn("_effective_to",      F.lit(None).cast("date"))
    .withColumn("_is_current",        F.lit(True))
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_source_system",     F.lit(SOURCE_SYS))
    .withColumn("_source_entity",     F.lit(ENTITY_NAME))
)

# Keep only columns that exist in target
target_cols = [f.name for f in spark.table(TGT_TABLE).schema.fields]
# Keep data cols + SCD meta
data_cols   = [c for c in staging_df.columns if not c.startswith("_")]
keep_cols   = data_cols + [c for c in staging_df.columns if c.startswith("_") and c in target_cols]
staging_df  = staging_df.select([c for c in keep_cols if c in staging_df.columns])

staging_df.createOrReplaceTempView("_scd2_incoming")

# COMMAND ----------
# MAGIC %md ## 4. SCD2 MERGE
# MAGIC
# MAGIC Logic:
# MAGIC 1. **Match + Hash Changed** → expire existing record (`_effective_to` = yesterday, `_is_current` = false)
# MAGIC 2. **No Match** → insert new record
# MAGIC 3. **Match + Hash Same** → no action (idempotent)
# MAGIC
# MAGIC After MERGE, insert new versions for records that were just expired.

# COMMAND ----------

join_cond = " AND ".join([f"target.{k} = source.{k}" for k in NAT_KEYS])
yesterday = str((datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=1)))

# Step 1: Expire changed records
spark.sql(f"""
    MERGE INTO {TGT_TABLE} AS target
    USING _scd2_incoming AS source
    ON {join_cond}
    AND target._is_current = true
    WHEN MATCHED AND target._record_hash <> source._record_hash THEN
        UPDATE SET
            target._effective_to  = '{yesterday}',
            target._is_current    = false
""")
print("Step 1 complete: expired changed records.")

# Step 2: Insert new versions (new + changed)
spark.sql(f"""
    MERGE INTO {TGT_TABLE} AS target
    USING (
        SELECT s.*
        FROM _scd2_incoming s
        LEFT JOIN {TGT_TABLE} t
            ON {join_cond.replace('target.', 't.').replace('source.', 's.')}
            AND t._is_current = true
        WHERE t.{NAT_KEYS[0]} IS NULL          -- brand new key
           OR t._record_hash <> s._record_hash  -- changed attributes
    ) AS source
    ON {join_cond}
    AND target._is_current = true
    AND target._record_hash = source._record_hash
    WHEN NOT MATCHED THEN INSERT *
""")
print("Step 2 complete: inserted new/changed records.")

# COMMAND ----------
# MAGIC %md ## 5. Validate SCD2 Integrity

# COMMAND ----------

# Each natural key should have exactly one current record
dupes_df = spark.sql(f"""
    SELECT {', '.join(NAT_KEYS)}, COUNT(*) AS cnt
    FROM {TGT_TABLE}
    WHERE _is_current = true
    GROUP BY {', '.join(NAT_KEYS)}
    HAVING cnt > 1
""")
dupe_count = dupes_df.count()
if dupe_count > 0:
    print(f"WARNING: {dupe_count} natural key(s) have multiple current records:")
    dupes_df.show(10, truncate=False)
else:
    print("SCD2 integrity check passed — no duplicate current records.")

# COMMAND ----------
# MAGIC %md ## 6. Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY ({NAT_KEYS[0]}, _effective_from)")

# COMMAND ----------
# MAGIC %md ## 7. Row Count Summary

# COMMAND ----------

total_in_table = spark.table(TGT_TABLE).count()
current_records = spark.table(TGT_TABLE).filter(F.col("_is_current") == True).count()
historical_records = total_in_table - current_records

print(f"\n{'='*50}")
print(f"SCD2 MERGE COMPLETE")
print(f"{'='*50}")
print(f"  Incoming rows this batch : {rows_incoming:,}")
print(f"  Total rows in table      : {total_in_table:,}")
print(f"  Current records (_is_current=true) : {current_records:,}")
print(f"  Historical records                 : {historical_records:,}")
print(f"{'='*50}")

dbutils.jobs.taskValues.set(key="silver_table",       value=f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}_history")
dbutils.jobs.taskValues.set(key="current_records",    value=current_records)
dbutils.jobs.taskValues.set(key="total_records",      value=total_in_table)
dbutils.jobs.taskValues.set(key="run_id",             value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{current_records}")
