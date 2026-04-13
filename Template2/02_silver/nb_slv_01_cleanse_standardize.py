# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_01_cleanse_standardize
# MAGIC **Layer:** Silver | **Pattern:** Cleanse & Standardize (Full Overwrite per batch)
# MAGIC **Purpose:** Read bronze, apply DQ checks, cast types, standardise, deduplicate, write to silver.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYSTEM = "crm"
ENTITY_NAME   = "customers"
DOMAIN        = "crm"
NATURAL_KEYS  = ["customer_id"]
BATCH_DATE    = str(__import__("datetime").date.today())

# Column → target type (edit to match your schema)
TYPE_CAST_MAP = {
    "birth_date":      "date",
    "created_at":      "timestamp",
    "annual_revenue":  "double",
    "employee_count":  "int",
}

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F, Window

RUN_ID      = str(uuid.uuid4())
SRC_TABLE   = f"`{CATALOG}`.`bronze`.`{SOURCE_SYSTEM}__{ENTITY_NAME}`"
TGT_TABLE   = f"`{CATALOG}`.`silver`.`{DOMAIN}__{ENTITY_NAME}`"

print(f"Source : {SRC_TABLE}")
print(f"Target : {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 1. Read Bronze

# COMMAND ----------

bronze_df = spark.table(SRC_TABLE).filter(
    F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date")
)
rows_read = bronze_df.count()
print(f"Bronze rows for {BATCH_DATE}: {rows_read:,}")

meta_cols = [c for c in bronze_df.columns if c.startswith("_")]
data_df   = bronze_df.drop(*meta_cols)

# COMMAND ----------
# MAGIC %md ## 2. Trim Whitespace & Replace Empty Strings with NULL

# COMMAND ----------

string_cols = [f.name for f in data_df.schema.fields if str(f.dataType) == "StringType()"]
clean_df    = data_df
for col in string_cols:
    clean_df = clean_df.withColumn(col, F.trim(F.col(col)))
    clean_df = clean_df.withColumn(col,
        F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col))
    )
print(f"Cleaned {len(string_cols)} string column(s).")

# COMMAND ----------
# MAGIC %md ## 3. Cast Data Types

# COMMAND ----------

typed_df = clean_df
for col_name, target_type in TYPE_CAST_MAP.items():
    if col_name in typed_df.columns:
        typed_df = typed_df.withColumn(col_name, F.col(col_name).cast(target_type))
        print(f"  Cast: {col_name} → {target_type}")

# COMMAND ----------
# MAGIC %md ## 4. Data Quality Checks

# COMMAND ----------

total_rows   = typed_df.count()
dq_results   = []

def run_check(name, check_type, col_name, condition, threshold):
    typed_df.createOrReplaceTempView("_dq_src")
    passing   = spark.sql(f"SELECT COUNT(*) AS c FROM _dq_src WHERE {condition}").collect()[0]["c"]
    pass_rate = passing / total_rows if total_rows > 0 else 1.0
    result    = "PASS" if pass_rate >= threshold else "FAIL"
    dq_results.append({
        "run_id": RUN_ID, "check_name": name, "entity_name": ENTITY_NAME,
        "layer": "silver", "check_type": check_type, "column_name": col_name,
        "rows_checked": total_rows, "rows_failed": total_rows - passing,
        "pass_rate": round(pass_rate, 4), "threshold": threshold,
        "result": result, "batch_date": BATCH_DATE,
    })
    icon = "✓" if result == "PASS" else "✗"
    print(f"  [{icon}] {name}: {result} (pass_rate={pass_rate:.2%})")
    return result

run_check(f"not_null__{NATURAL_KEYS[0]}", "not_null", NATURAL_KEYS[0],
          f"{NATURAL_KEYS[0]} IS NOT NULL", 1.0)

if "email" in typed_df.columns:
    run_check("valid_email__email", "regex", "email",
              r"email IS NULL OR email RLIKE '^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$'", 0.95)

if dq_results:
    spark.createDataFrame(dq_results).write.format("delta").mode("append") \
        .saveAsTable(f"{CATALOG}.audit.dq_check_results")

critical_failures = [r for r in dq_results if r["result"] == "FAIL" and r["threshold"] == 1.0]
if critical_failures:
    raise RuntimeError(f"Critical DQ failures: {[r['check_name'] for r in critical_failures]}")

# COMMAND ----------
# MAGIC %md ## 5. Deduplicate (Last-Write-Wins on Natural Key)

# COMMAND ----------

dedup_window = Window.partitionBy(*NATURAL_KEYS).orderBy(F.lit(1).desc())
dedup_df = (typed_df
    .withColumn("_row_rank", F.row_number().over(dedup_window))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")
)
print(f"Rows after dedup: {dedup_df.count():,} (removed {total_rows - dedup_df.count():,})")

# COMMAND ----------
# MAGIC %md ## 6. Add Silver Metadata & Write

# COMMAND ----------

silver_df = (dedup_df
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_system",     F.lit(SOURCE_SYSTEM))
    .withColumn("_source_entity",     F.lit(ENTITY_NAME))
    .withColumn("_is_current",        F.lit(True))
)

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_silver_batch_date = '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("_silver_batch_date")
    .saveAsTable(f"{CATALOG}.silver.{DOMAIN}__{ENTITY_NAME}")
)

rows_written = silver_df.count()
print(f"Written {rows_written:,} rows → {TGT_TABLE}")
spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY ({NATURAL_KEYS[0]})")
