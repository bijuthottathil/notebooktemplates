# Databricks notebook source
# MAGIC %md
# MAGIC # nb_01_bronze_ingest
# MAGIC **Layer:** Bronze
# MAGIC **Purpose:** Read all four health insurance CSV files from the ADLS landing zone and write
# MAGIC them as managed Delta tables in the bronze schema. Adds ingestion metadata to every table.
# MAGIC No business logic — raw data only.
# MAGIC
# MAGIC **Source CSVs → Bronze Delta Tables:**
# MAGIC ```
# MAGIC adls:.../landing/plans/      →  bronze.raw_plans
# MAGIC adls:.../landing/members/    →  bronze.raw_members
# MAGIC adls:.../landing/providers/  →  bronze.raw_providers
# MAGIC adls:.../landing/claims/     →  bronze.raw_claims
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## Utilities

# COMMAND ----------

# MAGIC %run ./utilities/nb_utils_audit_logger

# COMMAND ----------

# MAGIC %run ./utilities/nb_utils_data_quality

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── ADLS Gen2 (must match nb_00) ─────────────────────────────────────────────
STORAGE_ACCOUNT = "dlshealthdev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template1"
LANDING_PATH    = f"{ADLS_BASE}/landing"

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG    = "health_insurance_dev"
BATCH_DATE = "2024-07-30"

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
print(f"Run ID       : {RUN_ID}")
print(f"Batch Date   : {BATCH_DATE}")
print(f"Catalog      : {CATALOG}")
print(f"Landing Path : {LANDING_PATH}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration
# MAGIC
# MAGIC | Setting | Value | Rationale |
# MAGIC |---|---|---|
# MAGIC | `adaptive.enabled` | true | AQE re-plans at runtime using actual cardinality |
# MAGIC | `coalescePartitions.enabled` | true | Merges small partitions after wide transforms |
# MAGIC | `maxPartitionBytes` | 128 MB | Partition size when splitting large CSV files |
# MAGIC | `optimizeWrite` | true | Delta rewrites small files into right-sized Parquet |
# MAGIC | `autoCompact` | true | Delta compacts after writes to prevent file sprawl |

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                   "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
spark.conf.set("spark.sql.files.maxPartitionBytes",            str(128 * 1024 * 1024))
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",   "true")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## Helper — Read CSV from ADLS & Add Bronze Metadata

# COMMAND ----------

def ingest_csv(table_name, landing_folder):
    """Read CSV from ADLS landing zone, add metadata columns, write to bronze Delta table."""
    csv_path = f"{LANDING_PATH}/{landing_folder}/"

    df = (spark.read
          .option("header",                   "true")
          .option("inferSchema",              "true")   # use explicit schema in production
          .option("nullValue",                "")
          .option("multiLine",               "false")
          .option("ignoreLeadingWhiteSpace",  "true")
          .option("ignoreTrailingWhiteSpace", "true")
          .csv(csv_path))

    row_count = df.count()

    bronze_df = (df
        .withColumn("_ingest_id",   F.lit(RUN_ID))
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_batch_date",  F.lit(BATCH_DATE).cast("date"))
        .withColumn("_source_file", F.input_file_name())
    )

    (bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema",                  "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .saveAsTable(f"{CATALOG}.bronze.{table_name}"))

    print(f"  [OK] {CATALOG}.bronze.{table_name}  ({row_count:,} rows)")
    return row_count

# COMMAND ----------
# MAGIC %md ## Bronze Ingest — All Four Entities

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_01_bronze_ingest",
    layer="bronze", source_system="health_insurance",
    entity_name="all_entities", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    print("\n--- Ingesting: plans ---")
    rows_plans = ingest_csv("raw_plans", "plans")

    print("\n--- Ingesting: members ---")
    rows_members = ingest_csv("raw_members", "members")

    print("\n--- Ingesting: providers ---")
    rows_providers = ingest_csv("raw_providers", "providers")

    print("\n--- Ingesting: claims ---")
    rows_claims = ingest_csv("raw_claims", "claims")

    total_rows = rows_plans + rows_members + rows_providers + rows_claims
    audit.set_rows_read(total_rows)
    audit.set_rows_written(total_rows)

# COMMAND ----------
# MAGIC %md ## Optimize Bronze Delta Tables
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY` co-locates rows by key columns within each file.
# MAGIC Silver cleanse reads that filter on these columns skip far more files,
# MAGIC reducing scan cost significantly on larger datasets.

# COMMAND ----------

print("Running OPTIMIZE on bronze tables ...")

spark.sql(f"OPTIMIZE `{CATALOG}`.`bronze`.`raw_plans`"
          f" ZORDER BY (plan_id, plan_type)")

spark.sql(f"OPTIMIZE `{CATALOG}`.`bronze`.`raw_members`"
          f" ZORDER BY (member_id, plan_id, state)")

spark.sql(f"OPTIMIZE `{CATALOG}`.`bronze`.`raw_providers`"
          f" ZORDER BY (provider_id, provider_type)")

spark.sql(f"OPTIMIZE `{CATALOG}`.`bronze`.`raw_claims`"
          f" ZORDER BY (member_id, claim_date, claim_type)")

print("OPTIMIZE complete.")

# COMMAND ----------
# MAGIC %md ## DQ Checks — Bronze Tables

# COMMAND ----------

print("\n=== DQ: bronze.raw_plans ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_plans"),
    suite=[
        {"name": "not_null__plan_id",    "type": "not_null",            "column": "plan_id",    "threshold": 1.0},
        {"name": "unique__plan_id",      "type": "unique",              "column": "plan_id",    "threshold": 1.0},
        {"name": "not_null__plan_type",  "type": "not_null",            "column": "plan_type",  "threshold": 1.0},
        {"name": "valid_plan_type",      "type": "accepted_values",     "column": "plan_type",
         "values": ["PPO","HMO","EPO","POS","HDHP"],                                            "threshold": 1.0},
        {"name": "row_count_min_1",      "type": "row_count_threshold", "min_rows": 1,          "threshold": 1.0},
    ],
    entity_name="raw_plans", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_members ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_members"),
    suite=[
        {"name": "not_null__member_id",  "type": "not_null",            "column": "member_id",  "threshold": 1.0},
        {"name": "unique__member_id",    "type": "unique",              "column": "member_id",  "threshold": 1.0},
        {"name": "not_null__plan_id",    "type": "not_null",            "column": "plan_id",    "threshold": 1.0},
        {"name": "not_null__last_name",  "type": "not_null",            "column": "last_name",  "threshold": 1.0},
        {"name": "valid_gender",         "type": "accepted_values",     "column": "gender",
         "values": ["M","F","O","U"],                                                           "threshold": 1.0},
        {"name": "row_count_min_1",      "type": "row_count_threshold", "min_rows": 1,          "threshold": 1.0},
    ],
    entity_name="raw_members", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_providers ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_providers"),
    suite=[
        {"name": "not_null__provider_id",   "type": "not_null",            "column": "provider_id",   "threshold": 1.0},
        {"name": "unique__provider_id",     "type": "unique",              "column": "provider_id",   "threshold": 1.0},
        {"name": "not_null__provider_name", "type": "not_null",            "column": "provider_name", "threshold": 1.0},
        {"name": "valid_network",           "type": "accepted_values",     "column": "is_in_network",
         "values": ["true","false"],                                                                   "threshold": 1.0},
        {"name": "row_count_min_1",         "type": "row_count_threshold", "min_rows": 1,             "threshold": 1.0},
    ],
    entity_name="raw_providers", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_claims ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_claims"),
    suite=[
        {"name": "not_null__claim_id",   "type": "not_null",            "column": "claim_id",    "threshold": 1.0},
        {"name": "unique__claim_id",     "type": "unique",              "column": "claim_id",    "threshold": 1.0},
        {"name": "not_null__member_id",  "type": "not_null",            "column": "member_id",   "threshold": 1.0},
        {"name": "not_null__provider_id","type": "not_null",            "column": "provider_id", "threshold": 1.0},
        {"name": "valid_claim_type",     "type": "accepted_values",     "column": "claim_type",
         "values": ["MEDICAL","PHARMACY","DENTAL","VISION"],                                     "threshold": 1.0},
        {"name": "valid_claim_status",   "type": "accepted_values",     "column": "claim_status",
         "values": ["PAID","PENDING","DENIED","VOID"],                                           "threshold": 1.0},
        {"name": "row_count_min_1",      "type": "row_count_threshold", "min_rows": 1,           "threshold": 1.0},
    ],
    entity_name="raw_claims", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## Preview Bronze Tables

# COMMAND ----------

print("=== bronze.raw_members (sample) ===")
display(spark.table(f"{CATALOG}.bronze.raw_members").limit(5))

print("=== bronze.raw_claims (sample) ===")
display(spark.table(f"{CATALOG}.bronze.raw_claims").limit(5))

# COMMAND ----------

print("\nBronze ingestion complete.")
print(f"  raw_plans     : {rows_plans} rows")
print(f"  raw_members   : {rows_members} rows")
print(f"  raw_providers : {rows_providers} rows")
print(f"  raw_claims    : {rows_claims} rows")
print(f"\n  Physical storage : {ADLS_BASE}/catalog/{CATALOG}/bronze/")
print("\nNext: Run nb_02_silver_cleanse.py")
