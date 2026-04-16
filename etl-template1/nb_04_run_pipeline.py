# Databricks notebook source
# MAGIC %md
# MAGIC # nb_04_run_pipeline
# MAGIC **Purpose:** Single-click orchestrator. Runs all three pipeline notebooks in sequence:
# MAGIC ```
# MAGIC nb_01_bronze_ingest  →  nb_02_silver_cleanse  →  nb_03_gold_mart
# MAGIC ```
# MAGIC Run this after `nb_00_setup_and_load_data` has been executed at least once.

# COMMAND ----------
# MAGIC %md ## Utilities

# COMMAND ----------

# MAGIC %run ./utilities/nb_utils_audit_logger

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── ADLS Gen2 (must match nb_00) ─────────────────────────────────────────────
STORAGE_ACCOUNT = "dlshealthdev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template1"
LANDING_PATH    = f"{ADLS_BASE}/landing"

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG       = "health_insurance_dev"
BATCH_DATE    = "2024-07-30"
NOTEBOOK_BASE = "."   # "." means same folder as this notebook

# ─────────────────────────────────────────────────────────────────────────────

import datetime

PARAMS = {
    "catalog":          CATALOG,
    "batch_date":       BATCH_DATE,
    "storage_account":  STORAGE_ACCOUNT,
    "container":        CONTAINER,
    "adls_base":        ADLS_BASE,
    "landing_path":     LANDING_PATH,
}
start = datetime.datetime.utcnow()

print(f"Pipeline start  : {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Catalog         : {CATALOG}")
print(f"Batch Date      : {BATCH_DATE}")
print(f"Storage Account : {STORAGE_ACCOUNT}")
print(f"Landing Path    : {LANDING_PATH}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## Step 1 — Bronze Ingest

# COMMAND ----------

print("Running nb_01_bronze_ingest ...")
result_bronze = dbutils.notebook.run(f"{NOTEBOOK_BASE}/nb_01_bronze_ingest", timeout_seconds=600, arguments=PARAMS)
print(f"  Bronze result: {result_bronze}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Silver Cleanse

# COMMAND ----------

print("Running nb_02_silver_cleanse ...")
result_silver = dbutils.notebook.run(f"{NOTEBOOK_BASE}/nb_02_silver_cleanse", timeout_seconds=600, arguments=PARAMS)
print(f"  Silver result: {result_silver}")

# COMMAND ----------
# MAGIC %md ## Step 3 — Gold Mart

# COMMAND ----------

print("Running nb_03_gold_mart ...")
result_gold = dbutils.notebook.run(f"{NOTEBOOK_BASE}/nb_03_gold_mart", timeout_seconds=600, arguments=PARAMS)
print(f"  Gold result: {result_gold}")

# COMMAND ----------
# MAGIC %md ## Pipeline Summary

# COMMAND ----------

elapsed = (datetime.datetime.utcnow() - start).total_seconds()

print("\n" + "="*55)
print("PIPELINE COMPLETE")
print("="*55)
print(f"  Bronze  : {result_bronze}")
print(f"  Silver  : {result_silver}")
print(f"  Gold    : {result_gold}")
print(f"  Elapsed : {elapsed:.1f}s")
print("="*55)

# COMMAND ----------
# MAGIC %md ## Audit History

# COMMAND ----------

display(show_pipeline_history(catalog=CATALOG, days=1))

# COMMAND ----------
# MAGIC %md ## Recent DQ Results

# COMMAND ----------

display(spark.sql(f"""
    SELECT entity_name, layer, check_name, result, pass_rate, rows_failed, batch_date
    FROM `{CATALOG}`.`audit`.`dq_check_results`
    ORDER BY batch_date DESC, entity_name, layer
    LIMIT 50
"""))
