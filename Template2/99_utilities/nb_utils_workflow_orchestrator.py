# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils_workflow_orchestrator
# MAGIC **Layer:** Utilities
# MAGIC **Purpose:** Chain bronze → silver → gold notebooks in sequence using dbutils.notebook.run().
# MAGIC For production, prefer Databricks Workflows (Jobs UI) with task dependencies.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYSTEM = "crm"
ENTITY_NAME   = "customers"
BATCH_DATE    = str(__import__("datetime").date.today())
RUN_LAYER     = "all"                  # all | bronze | silver | gold

NOTEBOOK_TIMEOUT = 3600               # seconds per child notebook

# ─────────────────────────────────────────────────────────────────────────────

BASE_PARAMS = {
    "catalog":       CATALOG,
    "source_system": SOURCE_SYSTEM,
    "entity_name":   ENTITY_NAME,
    "batch_date":    BATCH_DATE,
}

print(f"Orchestrating: {SOURCE_SYSTEM}.{ENTITY_NAME} | {BATCH_DATE} | layers={RUN_LAYER}")

# COMMAND ----------
# MAGIC %md ## Pipeline Steps
# MAGIC Edit the PIPELINE list to match your actual notebook paths and parameter requirements.

# COMMAND ----------

# (layer, notebook_relative_path, extra_params)
PIPELINE = [
    ("bronze", "../01_bronze/nb_brz_01_full_load_batch",     {}),
    ("silver", "../02_silver/nb_slv_01_cleanse_standardize", {"natural_key_cols": "customer_id", "domain": SOURCE_SYSTEM}),
    ("gold",   "../03_gold/nb_gld_02_dimension_table_batch",  {"dim_name": ENTITY_NAME, "source_table": f"{SOURCE_SYSTEM}__{ENTITY_NAME}"}),
]

results = {}

for layer, nb_path, extra in PIPELINE:
    if RUN_LAYER not in ("all", layer):
        print(f"  [SKIP] {layer}: {nb_path}")
        continue

    params = {**BASE_PARAMS, **extra}
    print(f"\n>>> {layer.upper()}: {nb_path}")

    try:
        exit_value        = dbutils.notebook.run(nb_path, timeout_seconds=NOTEBOOK_TIMEOUT, arguments=params)
        results[layer]    = {"status": "SUCCESS", "exit": exit_value}
        print(f"    Result: {exit_value}")
    except Exception as e:
        results[layer]    = {"status": "FAILED", "error": str(e)}
        print(f"    FAILED: {e}")
        raise

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("ORCHESTRATION SUMMARY")
print("=" * 50)
for layer, r in results.items():
    detail = r.get("exit", r.get("error", ""))
    print(f"  {r['status']:8s} | {layer:8s} | {detail}")
print("=" * 50)
