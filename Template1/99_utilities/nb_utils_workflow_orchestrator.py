# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils_workflow_orchestrator
# MAGIC **Layer:** Utilities
# MAGIC **Purpose:** Master orchestrator notebook. Calls bronze → silver → gold notebooks in sequence
# MAGIC using `dbutils.notebook.run()`. Use this pattern for non-Databricks-Workflow environments
# MAGIC or when chaining notebooks within a single job task.
# MAGIC
# MAGIC **For production pipelines, prefer Databricks Workflows (Jobs UI) with task dependencies.**
# MAGIC This notebook is a convenience wrapper for development and migration testing.

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",       "company_dev",  "Catalog")
dbutils.widgets.text("batch_date",    "",             "Batch Date (YYYY-MM-DD, blank=today)")
dbutils.widgets.text("source_system", "crm",          "Source System")
dbutils.widgets.text("entity_name",   "customers",    "Entity Name")
dbutils.widgets.dropdown("run_layer", "all", ["all", "bronze", "silver", "gold"], "Run Layer")
dbutils.widgets.dropdown("env",       "dev", ["dev", "staging", "prod"], "Environment")

import datetime

CATALOG       = dbutils.widgets.get("catalog")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")
ENTITY_NAME   = dbutils.widgets.get("entity_name")
RUN_LAYER     = dbutils.widgets.get("run_layer")

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())

BASE_PARAMS   = {
    "catalog":       CATALOG,
    "source_system": SOURCE_SYSTEM,
    "entity_name":   ENTITY_NAME,
    "batch_date":    BATCH_DATE,
}

NOTEBOOK_TIMEOUT = 3600  # seconds per notebook

print(f"Orchestrating: {SOURCE_SYSTEM}.{ENTITY_NAME} | {BATCH_DATE} | layers={RUN_LAYER}")

# COMMAND ----------
# MAGIC %md ## Pipeline Steps

# COMMAND ----------

# ── Define pipeline steps ─────────────────────────────────────────────────────
# Each step: (layer, notebook_relative_path, extra_params)
PIPELINE = [
    ("bronze", "../01_bronze/nb_brz_01_full_load_batch",    {}),
    ("silver", "../02_silver/nb_slv_01_cleanse_standardize",{"natural_key_cols": "customer_id", "domain": SOURCE_SYSTEM}),
    ("gold",   "../03_gold/nb_gld_02_dimension_table_batch", {"dim_name": ENTITY_NAME, "source_table": f"{SOURCE_SYSTEM}__{ENTITY_NAME}"}),
]

results = {}

for layer, nb_path, extra in PIPELINE:
    if RUN_LAYER not in ("all", layer):
        print(f"  [SKIP] {layer}: {nb_path}")
        continue

    params = {**BASE_PARAMS, **extra}
    print(f"\n>>> Running {layer}: {nb_path}")
    print(f"    Params: {params}")

    try:
        exit_value = dbutils.notebook.run(nb_path, timeout_seconds=NOTEBOOK_TIMEOUT, arguments=params)
        results[layer] = {"status": "SUCCESS", "exit": exit_value}
        print(f"    Result: {exit_value}")
    except Exception as e:
        results[layer] = {"status": "FAILED", "error": str(e)}
        print(f"    FAILED: {e}")
        raise  # Fail fast — do not continue downstream layers if upstream fails

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "=" * 55)
print("ORCHESTRATION SUMMARY")
print("=" * 55)
for layer, r in results.items():
    status = r["status"]
    detail = r.get("exit", r.get("error", ""))
    print(f"  {status:8s} | {layer:8s} | {detail}")
print("=" * 55)

dbutils.notebook.exit("SUCCESS")
