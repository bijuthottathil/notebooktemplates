# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_02_scd_type2_batch
# MAGIC **Layer:** Silver | **Pattern:** Slowly Changing Dimension Type 2 (SCD2)
# MAGIC **Purpose:** Maintain full change history for dimension records using Delta MERGE.
# MAGIC Expired records get _effective_to set; new versions are inserted with _is_current = true.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG      = "company_dev"
SOURCE_SYS   = "crm"
ENTITY_NAME  = "customers"
DOMAIN       = "crm"
NAT_KEYS     = ["customer_id"]                                       # Natural key column(s)
TRACKED_COLS = ["customer_name", "email", "segment", "country"]     # Columns that trigger a new SCD2 version
BATCH_DATE   = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F

RUN_ID    = str(uuid.uuid4())
SRC_TABLE = f"`{CATALOG}`.`bronze`.`{SOURCE_SYS}__{ENTITY_NAME}`"
TGT_TABLE = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}_history`"
YESTERDAY = str(datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=1))

print(f"Source  : {SRC_TABLE}")
print(f"Target  : {TGT_TABLE}")
print(f"Keys    : {NAT_KEYS}")
print(f"Tracked : {TRACKED_COLS}")

# COMMAND ----------
# MAGIC %md ## 1. Create SCD2 Target Table (if not exists)

# COMMAND ----------

key_col_ddl = "\n".join([f"    {k}  STRING  NOT NULL," for k in NAT_KEYS])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE} (
    {key_col_ddl}
        _effective_from    DATE      NOT NULL,
        _effective_to      DATE,
        _is_current        BOOLEAN   NOT NULL,
        _record_hash       STRING,
        _silver_ingest_id  STRING,
        _silver_ingest_ts  TIMESTAMP,
        _source_system     STRING,
        _source_entity     STRING
    )
    USING DELTA
    COMMENT 'SCD2 history for {ENTITY_NAME}'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f"SCD2 table ready: {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 2. Read Incoming Batch from Bronze

# COMMAND ----------

incoming_df = spark.table(SRC_TABLE).filter(
    F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date")
)
print(f"Incoming rows: {incoming_df.count():,}")

# Build record hash from tracked columns
tracked_present = [c for c in TRACKED_COLS if c in incoming_df.columns]
hash_expr = F.md5(F.concat_ws("||", *[
    F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in tracked_present
]))

meta_cols   = [c for c in incoming_df.columns if c.startswith("_")]
staging_df  = (incoming_df.drop(*meta_cols)
    .withColumn("_record_hash",       hash_expr)
    .withColumn("_effective_from",    F.lit(BATCH_DATE).cast("date"))
    .withColumn("_effective_to",      F.lit(None).cast("date"))
    .withColumn("_is_current",        F.lit(True))
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_source_system",     F.lit(SOURCE_SYS))
    .withColumn("_source_entity",     F.lit(ENTITY_NAME))
)

staging_df.createOrReplaceTempView("_scd2_incoming")

# COMMAND ----------
# MAGIC %md ## 3. Step 1 — Expire Changed Records

# COMMAND ----------

join_cond = " AND ".join([f"target.{k} = source.{k}" for k in NAT_KEYS])

spark.sql(f"""
    MERGE INTO {TGT_TABLE} AS target
    USING _scd2_incoming AS source
    ON {join_cond} AND target._is_current = true
    WHEN MATCHED AND target._record_hash <> source._record_hash THEN
        UPDATE SET target._effective_to = '{YESTERDAY}', target._is_current = false
""")
print("Step 1 done: expired changed records.")

# COMMAND ----------
# MAGIC %md ## 4. Step 2 — Insert New / Changed Versions

# COMMAND ----------

tgt_alias_join = join_cond.replace("target.", "t.").replace("source.", "s.")

spark.sql(f"""
    MERGE INTO {TGT_TABLE} AS target
    USING (
        SELECT s.*
        FROM _scd2_incoming s
        LEFT JOIN {TGT_TABLE} t
            ON {tgt_alias_join} AND t._is_current = true
        WHERE t.{NAT_KEYS[0]} IS NULL OR t._record_hash <> s._record_hash
    ) AS source
    ON {join_cond} AND target._is_current = true AND target._record_hash = source._record_hash
    WHEN NOT MATCHED THEN INSERT *
""")
print("Step 2 done: inserted new/changed versions.")

# COMMAND ----------
# MAGIC %md ## 5. Integrity Check & Optimize

# COMMAND ----------

dupes = spark.sql(f"""
    SELECT {', '.join(NAT_KEYS)}, COUNT(*) AS cnt
    FROM {TGT_TABLE}
    WHERE _is_current = true
    GROUP BY {', '.join(NAT_KEYS)}
    HAVING cnt > 1
""").count()

if dupes > 0:
    print(f"WARNING: {dupes} key(s) have multiple current records — investigate.")
else:
    print("Integrity check passed — no duplicate current records.")

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY ({NAT_KEYS[0]}, _effective_from)")

total      = spark.table(TGT_TABLE).count()
current    = spark.table(TGT_TABLE).filter(F.col("_is_current") == True).count()
historical = total - current
print(f"\nTotal rows: {total:,} | Current: {current:,} | Historical: {historical:,}")
