# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_03_catalog_schema_setup
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Provision Unity Catalog, databases (schemas), and baseline table properties
# MAGIC **Run Order:** Must run after `nb_env_02_external_locations`
# MAGIC
# MAGIC **Catalog / Schema Hierarchy:**
# MAGIC ```
# MAGIC <catalog>                    e.g.  company_dev
# MAGIC   ├── bronze                        raw ingested data
# MAGIC   │     └── <domain>_<source>
# MAGIC   ├── silver                        cleansed / conformed
# MAGIC   │     └── <domain>
# MAGIC   ├── gold                          aggregated marts
# MAGIC   │     └── <domain>_mart
# MAGIC   └── audit                         pipeline run metadata
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("catalog_prefix", "company", "Catalog Prefix")
dbutils.widgets.text("bronze_ext_location", "ext_loc_bronze_dev", "Bronze External Location")
dbutils.widgets.text("silver_ext_location", "ext_loc_silver_dev", "Silver External Location")
dbutils.widgets.text("gold_ext_location",   "ext_loc_gold_dev",   "Gold External Location")

ENV             = dbutils.widgets.get("env")
CATALOG_PREFIX  = dbutils.widgets.get("catalog_prefix")
CATALOG_NAME    = f"{CATALOG_PREFIX}_{ENV}"

BRONZE_EXT_LOC  = dbutils.widgets.get("bronze_ext_location")
SILVER_EXT_LOC  = dbutils.widgets.get("silver_ext_location")
GOLD_EXT_LOC    = dbutils.widgets.get("gold_ext_location")

# Schema definitions: schema_name → (external_location, comment)
SCHEMAS = {
    "bronze":       (BRONZE_EXT_LOC, "Raw ingested data — no transformations applied"),
    "silver":       (SILVER_EXT_LOC, "Cleansed, conformed, deduplicated data"),
    "gold":         (GOLD_EXT_LOC,   "Aggregated business marts and report tables"),
    "audit":        (BRONZE_EXT_LOC, "Pipeline run logs and data quality results"),
}

print(f"Catalog : {CATALOG_NAME}")
print(f"Schemas : {list(SCHEMAS.keys())}")

# COMMAND ----------
# MAGIC %md ## 2. Create Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
    COMMENT 'Unity Catalog for {ENV} environment – managed by nb_env_03'
""")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
print(f"Catalog '{CATALOG_NAME}' ready.")

# COMMAND ----------
# MAGIC %md ## 3. Create Schemas (Databases)

# COMMAND ----------

for schema_name, (ext_loc, comment) in SCHEMAS.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{schema_name}`
        COMMENT '{comment}'
    """)
    print(f"  [OK] Schema: {CATALOG_NAME}.{schema_name}")

# COMMAND ----------
# MAGIC %md ## 4. Create Audit / Pipeline Run Log Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG_NAME}`.`audit`.`pipeline_run_log` (
        run_id            STRING         COMMENT 'UUID for this pipeline run',
        pipeline_name     STRING         COMMENT 'Notebook or workflow name',
        layer             STRING         COMMENT 'bronze | silver | gold',
        source_system     STRING         COMMENT 'Source system identifier',
        entity_name       STRING         COMMENT 'Table or entity being loaded',
        batch_date        DATE           COMMENT 'Business date of the batch',
        load_type         STRING         COMMENT 'full | incremental | scd2 | merge',
        status            STRING         COMMENT 'RUNNING | SUCCESS | FAILED',
        rows_read         BIGINT         COMMENT 'Records read from source',
        rows_written      BIGINT         COMMENT 'Records written to target',
        rows_rejected     BIGINT         COMMENT 'Records failed DQ checks',
        start_ts          TIMESTAMP      COMMENT 'Pipeline start time (UTC)',
        end_ts            TIMESTAMP      COMMENT 'Pipeline end time (UTC)',
        duration_seconds  DOUBLE         COMMENT 'Elapsed time in seconds',
        error_message     STRING         COMMENT 'Error details if failed',
        notebook_path     STRING         COMMENT 'Databricks notebook path',
        created_at        TIMESTAMP      DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Central audit log for all pipeline runs across all layers'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false',
        'pipelines.autoOptimize.zOrderCols' = 'batch_date,pipeline_name'
    )
""")
print(f"Audit table ready: {CATALOG_NAME}.audit.pipeline_run_log")

# COMMAND ----------
# MAGIC %md ## 5. Create Data Quality Results Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG_NAME}`.`audit`.`dq_check_results` (
        run_id          STRING,
        check_name      STRING         COMMENT 'e.g. not_null__customer_id',
        entity_name     STRING,
        layer           STRING,
        check_type      STRING         COMMENT 'not_null | unique | range | regex | referential',
        column_name     STRING,
        rows_checked    BIGINT,
        rows_failed     BIGINT,
        pass_rate       DOUBLE         COMMENT '0.0 – 1.0',
        threshold       DOUBLE         COMMENT 'Minimum acceptable pass rate',
        result          STRING         COMMENT 'PASS | FAIL | WARN',
        batch_date      DATE,
        checked_at      TIMESTAMP      DEFAULT current_timestamp()
    )
    USING DELTA
    COMMENT 'Data quality check results per pipeline run'
""")
print(f"DQ results table ready: {CATALOG_NAME}.audit.dq_check_results")

# COMMAND ----------
# MAGIC %md ## 6. Set Default Spark Configs for Delta

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

print("Delta optimizations enabled: autoMerge, optimizeWrite, autoCompact")

# COMMAND ----------
# MAGIC %md ## 7. Output Summary

# COMMAND ----------

summary_df = spark.sql(f"SHOW SCHEMAS IN `{CATALOG_NAME}`")
display(summary_df)

print("=" * 60)
print("CATALOG & SCHEMA SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog : {CATALOG_NAME}")
for s in SCHEMAS:
    print(f"  Schema  : {CATALOG_NAME}.{s}")
print("=" * 60)
print("Next Step: Run bronze ingestion notebooks (01_bronze/)")
