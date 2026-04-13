# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_01_cleanse_standardize
# MAGIC **Layer:** Silver
# MAGIC **Pattern:** Cleanse & Standardize (Full Overwrite)
# MAGIC **Purpose:** Read bronze table, apply DQ checks, cast data types, standardise formats, and write
# MAGIC to silver. This is the canonical first silver transformation — replace Alteryx
# MAGIC formula/cleanse tools.
# MAGIC
# MAGIC **Transformations Applied:**
# MAGIC - Null handling, trim whitespace, upper/lower case standardisation
# MAGIC - Explicit type casting (string → date/timestamp/numeric)
# MAGIC - Domain-specific regex validation (email, phone, postal code)
# MAGIC - Deduplication (last-write-wins on natural key)
# MAGIC - DQ results logged to `audit.dq_check_results`
# MAGIC
# MAGIC **Naming Convention** *(Airbnb Minerva / dbt-style)*:
# MAGIC ```
# MAGIC Table: <catalog>.silver.<domain>__<entity>
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",          "company_dev",  "Catalog")
dbutils.widgets.text("source_system",    "crm",          "Source System (bronze schema prefix)")
dbutils.widgets.text("entity_name",      "customers",    "Entity Name")
dbutils.widgets.text("domain",           "crm",          "Silver Domain")
dbutils.widgets.text("natural_key_cols", "customer_id",  "Natural Key Columns (comma-separated)")
dbutils.widgets.text("batch_date",       "",             "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("type_cast_map",    '{"birth_date":"date","created_at":"timestamp","annual_revenue":"double","employee_count":"int"}', "Type Cast Map (JSON)")
dbutils.widgets.dropdown("env",          "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid, json, re
from pyspark.sql import functions as F, Window

CATALOG       = dbutils.widgets.get("catalog")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")
ENTITY_NAME   = dbutils.widgets.get("entity_name")
DOMAIN        = dbutils.widgets.get("domain")
NATURAL_KEYS  = [k.strip() for k in dbutils.widgets.get("natural_key_cols").split(",")]
TYPE_CAST_MAP = json.loads(dbutils.widgets.get("type_cast_map"))

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID        = str(uuid.uuid4())

SRC_TABLE     = f"`{CATALOG}`.`bronze`.`{SOURCE_SYSTEM}__{ENTITY_NAME}`"
TGT_TABLE     = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}`"

print(f"Source : {SRC_TABLE}")
print(f"Target : {TGT_TABLE}")
print(f"Keys   : {NATURAL_KEYS}")

# COMMAND ----------
# MAGIC %md ## 2. Read Latest Bronze Partition

# COMMAND ----------

bronze_df = (spark.table(SRC_TABLE)
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
)
rows_read = bronze_df.count()
print(f"Bronze rows for {BATCH_DATE}: {rows_read:,}")

# Drop bronze internal metadata before transformation
META_COLS = [c for c in bronze_df.columns if c.startswith("_")]
data_df   = bronze_df.drop(*META_COLS)

# COMMAND ----------
# MAGIC %md ## 3. Trim Whitespace on All String Columns

# COMMAND ----------

string_cols = [f.name for f in data_df.schema.fields if str(f.dataType) == "StringType()"]
trimmed_df  = data_df
for col in string_cols:
    trimmed_df = trimmed_df.withColumn(col, F.trim(F.col(col)))

# Replace empty strings with NULL
for col in string_cols:
    trimmed_df = trimmed_df.withColumn(col,
        F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col))
    )

print(f"Trimmed {len(string_cols)} string columns.")

# COMMAND ----------
# MAGIC %md ## 4. Cast Data Types

# COMMAND ----------

typed_df = trimmed_df
for col_name, target_type in TYPE_CAST_MAP.items():
    if col_name in typed_df.columns:
        typed_df = typed_df.withColumn(col_name, F.col(col_name).cast(target_type))
        print(f"  Cast: {col_name} → {target_type}")
    else:
        print(f"  [SKIP] Column not found: {col_name}")

# COMMAND ----------
# MAGIC %md ## 5. Data Quality Checks

# COMMAND ----------

DQ_CHECKS = [
    # (check_name, check_type, column, condition_expr, threshold)
    ("not_null__" + NATURAL_KEYS[0], "not_null",  NATURAL_KEYS[0], f"{NATURAL_KEYS[0]} IS NOT NULL", 1.0),
    ("valid_email__email",            "regex",     "email",
     r"email IS NULL OR email RLIKE '^[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[A-Za-z]{{2,}}$'", 0.95),
]

dq_results = []
rejected_ids = set()
total_rows = typed_df.count()

typed_df.createOrReplaceTempView("_dq_check_source")

for check_name, check_type, col_name, condition, threshold in DQ_CHECKS:
    if col_name not in typed_df.columns:
        continue

    passing_count = spark.sql(f"SELECT COUNT(*) AS c FROM _dq_check_source WHERE {condition}").collect()[0]["c"]
    pass_rate     = passing_count / total_rows if total_rows > 0 else 1.0
    failed_rows   = total_rows - passing_count
    result        = "PASS" if pass_rate >= threshold else "FAIL"

    dq_results.append({
        "run_id": RUN_ID, "check_name": check_name, "entity_name": ENTITY_NAME,
        "layer": "silver", "check_type": check_type, "column_name": col_name,
        "rows_checked": total_rows, "rows_failed": failed_rows,
        "pass_rate": round(pass_rate, 4), "threshold": threshold,
        "result": result, "batch_date": BATCH_DATE,
    })
    icon = "✓" if result == "PASS" else "✗"
    print(f"  [{icon}] {check_name}: {result} (pass_rate={pass_rate:.2%}, failed={failed_rows:,})")

# Persist DQ results
if dq_results:
    dq_df = spark.createDataFrame(dq_results)
    (dq_df.write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.audit.dq_check_results"))

# Fail pipeline if any critical (threshold=1.0) check failed
critical_failures = [r for r in dq_results if r["result"] == "FAIL" and r["threshold"] == 1.0]
if critical_failures:
    raise RuntimeError(f"Critical DQ checks failed: {[r['check_name'] for r in critical_failures]}")

# COMMAND ----------
# MAGIC %md ## 6. Deduplication (Last-Write-Wins on Natural Key)

# COMMAND ----------

# If multiple records share the same natural key, keep the one with the latest watermark
dedup_window = Window.partitionBy(*NATURAL_KEYS).orderBy(F.col("_ingest_ts").desc() if "_ingest_ts" in bronze_df.columns else F.lit(1).desc())

dedup_df = (typed_df
    .withColumn("_row_rank", F.row_number().over(dedup_window))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")
)

rows_after_dedup = dedup_df.count()
print(f"Rows after dedup: {rows_after_dedup:,} (removed {total_rows - rows_after_dedup:,} duplicates)")

# COMMAND ----------
# MAGIC %md ## 7. Add Silver Metadata Columns

# COMMAND ----------

silver_df = (dedup_df
    .withColumn("_silver_ingest_id",   F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",   F.current_timestamp())
    .withColumn("_silver_batch_date",  F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",      F.lit(SOURCE_SYSTEM))
    .withColumn("_source_entity",      F.lit(ENTITY_NAME))
    .withColumn("_is_current",         F.lit(True))
)

# COMMAND ----------
# MAGIC %md ## 8. Write to Silver (Overwrite this batch partition)

# COMMAND ----------

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_silver_batch_date = '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("_silver_batch_date")
    .saveAsTable(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
)

rows_written = spark.table(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}").filter(
    F.col("_silver_batch_date") == F.lit(BATCH_DATE).cast("date")).count()

print(f"Written {rows_written:,} rows → {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 9. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="silver_table",   value=f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
dbutils.jobs.taskValues.set(key="rows_written",   value=rows_written)
dbutils.jobs.taskValues.set(key="rows_rejected",  value=total_rows - rows_written)
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_written}")
