# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_03_multiformat_file_landing
# MAGIC **Layer:** Bronze
# MAGIC **Pattern:** Multi-Format File Landing (Auto-Detect)
# MAGIC **Purpose:** Ingest files of mixed formats from a single landing drop zone into bronze.
# MAGIC Auto-detects CSV, Excel, JSON, Parquet, ORC, Avro. Each file type is read with appropriate
# MAGIC options, metadata-enriched, and appended to a unified bronze Delta table.
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Source teams deliver files in inconsistent or changing formats
# MAGIC - Migrating Alteryx workflows that accepted multiple input connectors
# MAGIC - Landing zone has mixed file types (e.g., reports as Excel + data as CSV)
# MAGIC
# MAGIC **File Naming Convention (expected from source):**
# MAGIC ```
# MAGIC <entity>_<YYYYMMDD>[_<seq>].<ext>
# MAGIC e.g.: customers_20240415.csv
# MAGIC       products_20240415_001.xlsx
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",        "company_dev",       "Catalog")
dbutils.widgets.text("source_system",  "files",             "Source System")
dbutils.widgets.text("entity_name",    "product_catalog",   "Entity Name")
dbutils.widgets.text("landing_path",   "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/files/product_catalog/", "Landing Path")
dbutils.widgets.text("batch_date",     "",                  "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("sheet_name",     "Sheet1",            "Excel Sheet Name (if applicable)")
dbutils.widgets.dropdown("env",        "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid, re
from pyspark.sql import functions as F

CATALOG       = dbutils.widgets.get("catalog")
SOURCE_SYSTEM = dbutils.widgets.get("source_system")
ENTITY_NAME   = dbutils.widgets.get("entity_name")
LANDING_PATH  = dbutils.widgets.get("landing_path")
SHEET_NAME    = dbutils.widgets.get("sheet_name")

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID        = str(uuid.uuid4())
TARGET_SCHEMA = "bronze"
TARGET_TABLE  = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"

print(f"Landing Path : {LANDING_PATH}")
print(f"Target Table : {CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md ## 2. Discover Files in Landing Zone

# COMMAND ----------

FORMAT_MAP = {
    ".csv":     "csv",
    ".tsv":     "csv",
    ".txt":     "csv",
    ".json":    "json",
    ".jsonl":   "json",
    ".parquet": "parquet",
    ".orc":     "orc",
    ".avro":    "avro",
    ".xlsx":    "excel",
    ".xls":     "excel",
}

landing_files = dbutils.fs.ls(LANDING_PATH)
discovered    = []

for f in landing_files:
    ext = "." + f.name.lower().rsplit(".", 1)[-1] if "." in f.name else ""
    fmt = FORMAT_MAP.get(ext)
    if fmt:
        discovered.append({"path": f.path, "name": f.name, "format": fmt, "size_bytes": f.size})
    else:
        print(f"  [SKIP] Unsupported extension: {f.name}")

print(f"\nDiscovered {len(discovered)} processable file(s):")
for d in discovered:
    print(f"  {d['format']:8s} | {d['size_bytes']:>12,} bytes | {d['name']}")

if not discovered:
    print("No files to process.")
    dbutils.notebook.exit("NO_FILES|0")

# COMMAND ----------
# MAGIC %md ## 3. Read and Harmonise Each File

# COMMAND ----------

def to_snake_case(col_name):
    s = re.sub(r"[\s\-\.]+", "_", col_name.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().lstrip("_")

def read_file(file_info: dict, sheet_name: str = "Sheet1"):
    path = file_info["path"]
    fmt  = file_info["format"]

    if fmt == "csv":
        df = (spark.read
              .option("header",      "true")
              .option("inferSchema", "true")
              .option("multiLine",   "true")
              .option("escape",      '"')
              .option("sep",         "," if path.endswith(".csv") else "\t")
              .csv(path))

    elif fmt == "excel":
        df = (spark.read
              .format("com.crealytics.spark.excel")
              .option("header",      "true")
              .option("inferSchema", "true")
              .option("dataAddress", f"'{sheet_name}'!A1")
              .option("treatEmptyValuesAsNulls", "true")
              .load(path))

    elif fmt == "json":
        df = spark.read.option("multiLine", "true").json(path)

    elif fmt == "parquet":
        df = spark.read.parquet(path)

    elif fmt == "orc":
        df = spark.read.orc(path)

    elif fmt == "avro":
        df = spark.read.format("avro").load(path)

    else:
        raise ValueError(f"Unsupported format: {fmt}")

    # Normalise column names
    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)

    # Cast all columns to string for schema-on-read bronze pattern
    # Silver layer will cast to proper types
    for col in df.columns:
        df = df.withColumn(col, F.col(col).cast("string"))

    return df

# Collect all frames, union on common superset of columns
all_frames  = []
total_rows  = 0
parse_errors = []

for file_info in discovered:
    try:
        df       = read_file(file_info, SHEET_NAME)
        row_cnt  = df.count()
        total_rows += row_cnt
        df = (df
            .withColumn("_ingest_id",        F.lit(RUN_ID))
            .withColumn("_ingest_ts",        F.current_timestamp())
            .withColumn("_ingest_date",      F.lit(BATCH_DATE).cast("date"))
            .withColumn("_source_system",    F.lit(SOURCE_SYSTEM))
            .withColumn("_source_entity",    F.lit(ENTITY_NAME))
            .withColumn("_source_file_name", F.lit(file_info["name"]))
            .withColumn("_source_file_path", F.lit(file_info["path"]))
            .withColumn("_source_format",    F.lit(file_info["format"]))
            .withColumn("_load_type",        F.lit("file_landing"))
            .withColumn("_batch_date",       F.lit(BATCH_DATE).cast("date"))
        )
        all_frames.append(df)
        print(f"  [OK] {file_info['name']} — {row_cnt:,} rows")
    except Exception as e:
        parse_errors.append({"file": file_info["name"], "error": str(e)})
        print(f"  [ERR] {file_info['name']}: {e}")

print(f"\nTotal rows to write: {total_rows:,}")
print(f"Parse errors      : {len(parse_errors)}")

# COMMAND ----------
# MAGIC %md ## 4. Union All Frames (Schema Merge)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

def union_with_missing_cols(dfs: list) -> DataFrame:
    """Union DataFrames that may have different column sets — fills missing cols with NULL."""
    all_cols = set()
    for df in dfs:
        all_cols.update(df.columns)
    all_cols = sorted(all_cols)

    aligned = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        for m in missing:
            df = df.withColumn(m, F.lit(None).cast("string"))
        aligned.append(df.select(all_cols))

    return reduce(DataFrame.union, aligned)

combined_df = union_with_missing_cols(all_frames)
print(f"Combined schema: {len(combined_df.columns)} columns")
combined_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 5. Write to Bronze Delta Table (Append)

# COMMAND ----------

(combined_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)
print(f"Written {total_rows:,} rows → {CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")

# COMMAND ----------
# MAGIC %md ## 6. Archive Processed Files

# COMMAND ----------

ARCHIVE_BASE = LANDING_PATH.replace("/landing/", "/landing/_archive/")

for file_info in discovered:
    archive_path = ARCHIVE_BASE + BATCH_DATE + "/" + file_info["name"]
    try:
        dbutils.fs.cp(file_info["path"], archive_path)
        dbutils.fs.rm(file_info["path"])
        print(f"  [ARCHIVED] {file_info['name']}")
    except Exception as e:
        print(f"  [WARN] Archive failed for {file_info['name']}: {e}")

# COMMAND ----------
# MAGIC %md ## 7. Output

# COMMAND ----------

if parse_errors:
    print(f"\nWARNING: {len(parse_errors)} file(s) failed to parse:")
    for err in parse_errors:
        print(f"  {err['file']}: {err['error']}")

dbutils.jobs.taskValues.set(key="bronze_table", value=f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
dbutils.jobs.taskValues.set(key="rows_written", value=total_rows)
dbutils.jobs.taskValues.set(key="run_id",       value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{total_rows}")
