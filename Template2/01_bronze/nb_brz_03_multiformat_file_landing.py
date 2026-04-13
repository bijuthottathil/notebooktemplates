# Databricks notebook source
# MAGIC %md
# MAGIC # nb_brz_03_multiformat_file_landing
# MAGIC **Layer:** Bronze | **Pattern:** Multi-Format File Landing (Auto-Detect)
# MAGIC **Purpose:** Ingest mixed-format files (CSV, Excel, JSON, Parquet, ORC, Avro) from a single
# MAGIC landing folder. Auto-detects format by extension, unions all into one bronze Delta table.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
SOURCE_SYSTEM = "files"
ENTITY_NAME   = "product_catalog"
LANDING_PATH  = "abfss://datalake@dlscompanydev.dfs.core.windows.net/landing/files/product_catalog/"
SHEET_NAME    = "Sheet1"     # Excel sheet name (used only for Excel files)
BATCH_DATE    = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid, re
from pyspark.sql import functions as F, DataFrame
from functools import reduce

RUN_ID        = str(uuid.uuid4())
TARGET_SCHEMA = "bronze"
TARGET_TABLE  = f"{SOURCE_SYSTEM}__{ENTITY_NAME}"
FULL_TABLE    = f"`{CATALOG}`.`{TARGET_SCHEMA}`.`{TARGET_TABLE}`"

FORMAT_MAP = {
    ".csv": "csv", ".tsv": "csv", ".txt": "csv",
    ".json": "json", ".jsonl": "json",
    ".parquet": "parquet", ".orc": "orc", ".avro": "avro",
    ".xlsx": "excel", ".xls": "excel",
}

print(f"Landing Path : {LANDING_PATH}")
print(f"Target Table : {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 1. Discover Files

# COMMAND ----------

landing_files = dbutils.fs.ls(LANDING_PATH)
discovered    = []
for f in landing_files:
    ext = ("." + f.name.lower().rsplit(".", 1)[-1]) if "." in f.name else ""
    fmt = FORMAT_MAP.get(ext)
    if fmt:
        discovered.append({"path": f.path, "name": f.name, "format": fmt, "size_bytes": f.size})
    else:
        print(f"  [SKIP] {f.name}")

print(f"\nDiscovered {len(discovered)} file(s):")
for d in discovered:
    print(f"  {d['format']:8s} | {d['size_bytes']:>10,} bytes | {d['name']}")

if not discovered:
    dbutils.notebook.exit("NO_FILES|0")

# COMMAND ----------
# MAGIC %md ## 2. Read & Harmonise Each File

# COMMAND ----------

def to_snake_case(col_name):
    s = re.sub(r"[\s\-\.]+", "_", col_name.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().lstrip("_")

def read_file(file_info):
    path, fmt = file_info["path"], file_info["format"]
    if fmt == "csv":
        df = spark.read.option("header","true").option("inferSchema","true").option("multiLine","true").csv(path)
    elif fmt == "excel":
        df = (spark.read.format("com.crealytics.spark.excel")
              .option("header","true").option("inferSchema","true")
              .option("dataAddress", f"'{SHEET_NAME}'!A1")
              .option("treatEmptyValuesAsNulls","true").load(path))
    elif fmt == "json":
        df = spark.read.option("multiLine","true").json(path)
    elif fmt == "parquet":
        df = spark.read.parquet(path)
    elif fmt == "orc":
        df = spark.read.orc(path)
    elif fmt == "avro":
        df = spark.read.format("avro").load(path)
    else:
        raise ValueError(f"Unsupported: {fmt}")

    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)
    # Cast all to string for schema-on-read bronze pattern
    for col in df.columns:
        df = df.withColumn(col, F.col(col).cast("string"))
    return df

def union_all(dfs):
    all_cols = sorted(set(c for df in dfs for c in df.columns))
    aligned  = []
    for df in dfs:
        for c in all_cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        aligned.append(df.select(all_cols))
    return reduce(DataFrame.union, aligned)

all_frames   = []
total_rows   = 0
parse_errors = []

for file_info in discovered:
    try:
        df        = read_file(file_info)
        row_cnt   = df.count()
        total_rows += row_cnt
        df = (df
            .withColumn("_ingest_id",         F.lit(RUN_ID))
            .withColumn("_ingest_ts",         F.current_timestamp())
            .withColumn("_ingest_date",       F.lit(BATCH_DATE).cast("date"))
            .withColumn("_source_system",     F.lit(SOURCE_SYSTEM))
            .withColumn("_source_entity",     F.lit(ENTITY_NAME))
            .withColumn("_source_file_name",  F.lit(file_info["name"]))
            .withColumn("_source_file_path",  F.lit(file_info["path"]))
            .withColumn("_source_format",     F.lit(file_info["format"]))
            .withColumn("_load_type",         F.lit("file_landing"))
            .withColumn("_batch_date",        F.lit(BATCH_DATE).cast("date"))
        )
        all_frames.append(df)
        print(f"  [OK] {file_info['name']} — {row_cnt:,} rows")
    except Exception as e:
        parse_errors.append({"file": file_info["name"], "error": str(e)})
        print(f"  [ERR] {file_info['name']}: {e}")

print(f"\nTotal rows: {total_rows:,} | Errors: {len(parse_errors)}")

# COMMAND ----------
# MAGIC %md ## 3. Union & Write to Bronze

# COMMAND ----------

combined_df = union_all(all_frames)

(combined_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(f"{CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
)
print(f"Written {total_rows:,} rows → {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Archive Processed Files

# COMMAND ----------

ARCHIVE_BASE = LANDING_PATH.replace("/landing/", "/landing/_archive/")
for file_info in discovered:
    try:
        dbutils.fs.cp(file_info["path"], f"{ARCHIVE_BASE}{BATCH_DATE}/{file_info['name']}")
        dbutils.fs.rm(file_info["path"])
        print(f"  [ARCHIVED] {file_info['name']}")
    except Exception as e:
        print(f"  [WARN] {file_info['name']}: {e}")

if parse_errors:
    print(f"\nFiles with parse errors:")
    for err in parse_errors:
        print(f"  {err['file']}: {err['error']}")
