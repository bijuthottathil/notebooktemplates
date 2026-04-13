# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_03_catalog_schema_setup
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Provision Unity Catalog, schemas, and baseline audit tables
# MAGIC **Run Order:** Must run after nb_env_02_external_locations

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

ENV            = "dev"        # dev | staging | prod
CATALOG_PREFIX = "company"    # Resulting catalog: company_dev / company_staging / company_prod

# ─────────────────────────────────────────────────────────────────────────────

CATALOG_NAME = f"{CATALOG_PREFIX}_{ENV}"

SCHEMAS = {
    "bronze": "Raw ingested data — no transformations applied",
    "silver": "Cleansed, conformed, deduplicated data",
    "gold":   "Aggregated business marts and report tables",
    "audit":  "Pipeline run logs and data quality results",
}

print(f"Catalog : {CATALOG_NAME}")
print(f"Schemas : {list(SCHEMAS.keys())}")

# COMMAND ----------
# MAGIC %md ## 1. Create Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
    COMMENT 'Unity Catalog for {ENV} environment'
""")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
print(f"Catalog '{CATALOG_NAME}' ready.")

# COMMAND ----------
# MAGIC %md ## 2. Create Schemas

# COMMAND ----------

for schema_name, comment in SCHEMAS.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{schema_name}`
        COMMENT '{comment}'
    """)
    print(f"  [OK] {CATALOG_NAME}.{schema_name}")

# COMMAND ----------
# MAGIC %md ## 3. Create Audit Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG_NAME}`.`audit`.`pipeline_run_log` (
        run_id            STRING,
        pipeline_name     STRING,
        layer             STRING,
        source_system     STRING,
        entity_name       STRING,
        batch_date        DATE,
        load_type         STRING,
        status            STRING,
        rows_read         BIGINT,
        rows_written      BIGINT,
        rows_rejected     BIGINT,
        start_ts          TIMESTAMP,
        end_ts            TIMESTAMP,
        duration_seconds  DOUBLE,
        error_message     STRING,
        notebook_path     STRING,
        created_at        TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Central audit log for all pipeline runs'
    TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols' = 'batch_date,pipeline_name')
""")
print(f"Audit table ready: {CATALOG_NAME}.audit.pipeline_run_log")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG_NAME}`.`audit`.`dq_check_results` (
        run_id        STRING,
        check_name    STRING,
        entity_name   STRING,
        layer         STRING,
        check_type    STRING,
        column_name   STRING,
        rows_checked  BIGINT,
        rows_failed   BIGINT,
        pass_rate     DOUBLE,
        threshold     DOUBLE,
        result        STRING,
        batch_date    DATE,
        checked_at    TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Data quality check results per pipeline run'
""")
print(f"DQ table ready: {CATALOG_NAME}.audit.dq_check_results")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG_NAME}`.`audit`.`watermark_control` (
        source_system   STRING,
        entity_name     STRING,
        watermark_col   STRING,
        last_watermark  STRING,
        last_run_id     STRING,
        last_updated    TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'High-watermark tracking for incremental loads'
""")
print(f"Watermark table ready: {CATALOG_NAME}.audit.watermark_control")

# COMMAND ----------
# MAGIC %md ## 4. Default Spark Configs

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",    "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",      "true")
print("Delta optimizations enabled.")

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{CATALOG_NAME}`"))

print("=" * 55)
print("CATALOG & SCHEMA SETUP COMPLETE")
print("=" * 55)
print(f"  Catalog : {CATALOG_NAME}")
for s in SCHEMAS:
    print(f"  Schema  : {CATALOG_NAME}.{s}")
print("=" * 55)
print("Next Step: Run bronze ingestion notebooks (01_bronze/)")
