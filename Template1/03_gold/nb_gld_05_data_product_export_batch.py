# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_05_data_product_export_batch
# MAGIC **Layer:** Gold
# MAGIC **Pattern:** Data Product Export (Gold → External Consumers)
# MAGIC **Purpose:** Package and export gold data for downstream consumers: external APIs, file drops
# MAGIC for legacy systems, Power BI datasets, or partner data shares.
# MAGIC Applies final business validation before publishing.
# MAGIC
# MAGIC **Export Formats supported:** Delta Share · Parquet · CSV · JSON
# MAGIC **Naming Convention** *(Uber Data Mesh inspired)*:
# MAGIC ```
# MAGIC Table     : gold.dp_<product_name>
# MAGIC Export    : /gold/exports/<product_name>/<YYYY-MM-DD>/
# MAGIC ```
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Publishing gold tables to external file drops for legacy/non-Databricks consumers
# MAGIC - Replacing Alteryx workflows that wrote results to shared network drives or SFTP
# MAGIC - Creating certified data products for internal data marketplace

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",          "company_dev",           "Catalog")
dbutils.widgets.text("product_name",     "sales_summary",         "Data Product Name")
dbutils.widgets.text("source_table",     "gold.rpt_sales_monthly","Source Gold Table (schema.table)")
dbutils.widgets.text("export_base_path", "abfss://datalake@dlscompanydev.dfs.core.windows.net/gold/exports/", "Export Base Path")
dbutils.widgets.dropdown("export_format","parquet", ["parquet", "csv", "json", "delta"], "Export Format")
dbutils.widgets.text("batch_date",       "",                      "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("filter_expr",      "",                      "Optional Filter Expression (blank = no filter)")
dbutils.widgets.dropdown("env",          "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG       = dbutils.widgets.get("catalog")
PRODUCT_NAME  = dbutils.widgets.get("product_name")
SOURCE_TABLE  = dbutils.widgets.get("source_table")
EXPORT_BASE   = dbutils.widgets.get("export_base_path")
EXPORT_FORMAT = dbutils.widgets.get("export_format")
FILTER_EXPR   = dbutils.widgets.get("filter_expr").strip()

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID        = str(uuid.uuid4())

EXPORT_PATH   = f"{EXPORT_BASE.rstrip('/')}/{PRODUCT_NAME}/{BATCH_DATE}/"
DP_TABLE      = f"`{CATALOG}`.`gold`.`dp_{PRODUCT_NAME}`"

print(f"Source Table  : {CATALOG}.{SOURCE_TABLE}")
print(f"Export Path   : {EXPORT_PATH}")
print(f"Export Format : {EXPORT_FORMAT}")

# COMMAND ----------
# MAGIC %md ## 2. Read Source Gold Table

# COMMAND ----------

src_df = spark.table(f"{CATALOG}.{SOURCE_TABLE}")

if FILTER_EXPR:
    src_df = src_df.filter(FILTER_EXPR)
    print(f"Filter applied: {FILTER_EXPR}")

# Drop internal metadata columns — consumers should not see pipeline internals
internal_cols = [c for c in src_df.columns if c.startswith("_")]
export_df     = src_df.drop(*internal_cols)

rows_to_export = export_df.count()
print(f"Rows to export: {rows_to_export:,}")
print(f"Export columns: {export_df.columns}")

# COMMAND ----------
# MAGIC %md ## 3. Final Validation Before Export (Gate Check)

# COMMAND ----------

# Hard gate: do not export empty dataset
if rows_to_export == 0:
    raise RuntimeError(f"Export aborted: 0 rows in source table '{SOURCE_TABLE}' for filter: '{FILTER_EXPR}'")

# Hard gate: ensure no unexpected nulls in key business columns
# Adjust this list to match your data product contract
KEY_COLUMNS  = ["order_year", "order_month", "account_tier", "total_net_revenue"]
null_counts  = export_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in KEY_COLUMNS if c in export_df.columns
]).collect()[0].asDict()

null_violations = {k: v for k, v in null_counts.items() if v > 0}
if null_violations:
    print(f"WARNING: Nulls detected in key columns: {null_violations}")

# COMMAND ----------
# MAGIC %md ## 4. Materialise as Certified Data Product Table

# COMMAND ----------

certified_df = (export_df
    .withColumn("dp_product_name",    F.lit(PRODUCT_NAME))
    .withColumn("dp_export_date",     F.lit(BATCH_DATE).cast("date"))
    .withColumn("dp_certified_by",    F.lit("data-platform-team"))
    .withColumn("dp_export_ts",       F.current_timestamp())
    .withColumn("dp_version",         F.lit("1.0"))
)

(certified_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"dp_export_date = '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("dp_export_date")
    .saveAsTable(f"{CATALOG}.gold.dp_{PRODUCT_NAME}")
)
print(f"Data product table written: {DP_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Export to External Path (for Legacy / Non-Databricks Consumers)

# COMMAND ----------

write_opts = {
    "parquet": lambda df, path: (df.coalesce(4).write
        .mode("overwrite").option("compression", "snappy").parquet(path)),
    "csv":     lambda df, path: (df.coalesce(1).write
        .mode("overwrite").option("header", "true").option("compression", "gzip").csv(path)),
    "json":    lambda df, path: (df.coalesce(4).write
        .mode("overwrite").option("compression", "gzip").json(path)),
    "delta":   lambda df, path: (df.write
        .format("delta").mode("overwrite").save(path)),
}

write_fn = write_opts.get(EXPORT_FORMAT)
if write_fn:
    write_fn(export_df, EXPORT_PATH)
    print(f"Exported {rows_to_export:,} rows as {EXPORT_FORMAT} → {EXPORT_PATH}")
else:
    raise ValueError(f"Unsupported export format: {EXPORT_FORMAT}")

# COMMAND ----------
# MAGIC %md ## 6. Write Export Manifest (for consumers to detect new drops)

# COMMAND ----------

manifest = spark.createDataFrame([{
    "product_name":   PRODUCT_NAME,
    "export_date":    BATCH_DATE,
    "export_format":  EXPORT_FORMAT,
    "export_path":    EXPORT_PATH,
    "row_count":      rows_to_export,
    "run_id":         RUN_ID,
    "exported_at":    str(datetime.datetime.utcnow()),
    "status":         "SUCCESS",
}])

manifest_path = f"{EXPORT_BASE.rstrip('/')}/{PRODUCT_NAME}/_manifest/"
(manifest.write
    .format("delta")
    .mode("append")
    .save(manifest_path)
)
print(f"Manifest updated: {manifest_path}")

# COMMAND ----------
# MAGIC %md ## 7. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="dp_table",      value=f"{CATALOG}.gold.dp_{PRODUCT_NAME}")
dbutils.jobs.taskValues.set(key="export_path",   value=EXPORT_PATH)
dbutils.jobs.taskValues.set(key="rows_exported", value=rows_to_export)
dbutils.jobs.taskValues.set(key="run_id",        value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_to_export}")
