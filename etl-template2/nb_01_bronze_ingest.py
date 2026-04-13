# Databricks notebook source
# MAGIC %md
# MAGIC # nb_01_bronze_ingest
# MAGIC **Layer:** Bronze
# MAGIC **Purpose:** Read all four supply chain CSV files from the ADLS landing zone and write them
# MAGIC as managed Delta tables in the bronze schema. Adds ingestion metadata columns to every table.
# MAGIC No business logic — raw data only.
# MAGIC
# MAGIC **Source CSVs → Bronze Delta Tables:**
# MAGIC ```
# MAGIC adls:.../landing/suppliers/             →  bronze.raw_suppliers
# MAGIC adls:.../landing/products/              →  bronze.raw_products
# MAGIC adls:.../landing/purchase_orders/       →  bronze.raw_purchase_orders
# MAGIC adls:.../landing/purchase_order_lines/  →  bronze.raw_purchase_order_lines
# MAGIC ```
# MAGIC Delta tables are stored on ADLS via the catalog managed location set up in nb_00.

# COMMAND ----------
# MAGIC %md ## Utilities

# COMMAND ----------

# MAGIC %run ./99_utilities/nb_utils_audit_logger

# COMMAND ----------

# MAGIC %run ./99_utilities/nb_utils_data_quality

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── ADLS Gen2 (must match nb_00) ─────────────────────────────────────────────
STORAGE_ACCOUNT = "dlscompanydev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template2"
LANDING_PATH    = f"{ADLS_BASE}/landing"

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG         = "supply_chain_dev"
BATCH_DATE      = "2024-04-10"

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
print(f"Run ID       : {RUN_ID}")
print(f"Batch Date   : {BATCH_DATE}")
print(f"Catalog      : {CATALOG}")
print(f"Landing Path : {LANDING_PATH}")

# COMMAND ----------
# MAGIC %md ## Helper — Read CSV from ADLS & Add Bronze Metadata

# COMMAND ----------

def ingest_csv(table_name, landing_folder):
    """Read CSV from ADLS landing zone, add metadata columns, write to bronze Delta table."""

    csv_path = f"{LANDING_PATH}/{landing_folder}/"

    df = (spark.read
          .option("header",    "true")
          .option("inferSchema","true")
          .option("nullValue", "")
          .csv(csv_path))

    row_count = df.count()

    bronze_df = (df
        .withColumn("_ingest_id",   F.lit(RUN_ID))
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_batch_date",  F.lit(BATCH_DATE).cast("date"))
        .withColumn("_source_file", F.input_file_name())
    )

    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.bronze.{table_name}")

    print(f"  [OK] {CATALOG}.bronze.{table_name}  ({row_count:,} rows)")
    return row_count, bronze_df

# COMMAND ----------
# MAGIC %md ## Bronze Ingest — All Four Entities

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_01_bronze_ingest",
    layer="bronze", source_system="supply_chain",
    entity_name="all_entities", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    # ── 1. Suppliers ─────────────────────────────────────────────────────────
    print("\n--- Ingesting: suppliers ---")
    rows_suppliers, df_suppliers = ingest_csv("raw_suppliers", "suppliers")

    # ── 2. Products ──────────────────────────────────────────────────────────
    print("\n--- Ingesting: products ---")
    rows_products, df_products = ingest_csv("raw_products", "products")

    # ── 3. Purchase Orders ───────────────────────────────────────────────────
    print("\n--- Ingesting: purchase_orders ---")
    rows_po, df_po = ingest_csv("raw_purchase_orders", "purchase_orders")

    # ── 4. Purchase Order Lines ──────────────────────────────────────────────
    print("\n--- Ingesting: purchase_order_lines ---")
    rows_lines, df_lines = ingest_csv("raw_purchase_order_lines", "purchase_order_lines")

    total_rows = rows_suppliers + rows_products + rows_po + rows_lines
    audit.set_rows_read(total_rows)
    audit.set_rows_written(total_rows)

# COMMAND ----------
# MAGIC %md ## DQ Checks — Bronze Tables

# COMMAND ----------

print("\n=== DQ: bronze.raw_suppliers ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_suppliers"),
    suite=[
        {"name": "not_null__supplier_id",   "type": "not_null",            "column": "supplier_id",   "threshold": 1.0},
        {"name": "unique__supplier_id",     "type": "unique",              "column": "supplier_id",   "threshold": 1.0},
        {"name": "not_null__supplier_name", "type": "not_null",            "column": "supplier_name", "threshold": 1.0},
        {"name": "row_count_min_1",         "type": "row_count_threshold", "min_rows": 1,             "threshold": 1.0},
    ],
    entity_name="raw_suppliers", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_products ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_products"),
    suite=[
        {"name": "not_null__product_id",   "type": "not_null",            "column": "product_id",   "threshold": 1.0},
        {"name": "unique__product_id",     "type": "unique",              "column": "product_id",   "threshold": 1.0},
        {"name": "not_null__product_name", "type": "not_null",            "column": "product_name", "threshold": 1.0},
        {"name": "row_count_min_1",        "type": "row_count_threshold", "min_rows": 1,            "threshold": 1.0},
    ],
    entity_name="raw_products", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_purchase_orders ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_purchase_orders"),
    suite=[
        {"name": "not_null__po_id",         "type": "not_null",            "column": "po_id",        "threshold": 1.0},
        {"name": "unique__po_id",           "type": "unique",              "column": "po_id",        "threshold": 1.0},
        {"name": "not_null__supplier_id",   "type": "not_null",            "column": "supplier_id",  "threshold": 1.0},
        {"name": "valid_status",            "type": "accepted_values",     "column": "status",
         "values": ["DELIVERED","CANCELLED","IN_TRANSIT","APPROVED","PENDING"],             "threshold": 0.99},
        {"name": "row_count_min_1",         "type": "row_count_threshold", "min_rows": 1,            "threshold": 1.0},
    ],
    entity_name="raw_purchase_orders", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

print("\n=== DQ: bronze.raw_purchase_order_lines ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.bronze.raw_purchase_order_lines"),
    suite=[
        {"name": "not_null__line_id",  "type": "not_null",            "column": "line_id",  "threshold": 1.0},
        {"name": "unique__line_id",    "type": "unique",              "column": "line_id",  "threshold": 1.0},
        {"name": "not_null__po_id",    "type": "not_null",            "column": "po_id",    "threshold": 1.0},
        {"name": "not_null__product_id","type": "not_null",           "column": "product_id","threshold": 1.0},
        {"name": "row_count_min_1",    "type": "row_count_threshold", "min_rows": 1,        "threshold": 1.0},
    ],
    entity_name="raw_purchase_order_lines", layer="bronze",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## Preview Bronze Tables

# COMMAND ----------

print("=== bronze.raw_suppliers (sample) ===")
display(spark.table(f"{CATALOG}.bronze.raw_suppliers").limit(3))

print("=== bronze.raw_purchase_orders (sample) ===")
display(spark.table(f"{CATALOG}.bronze.raw_purchase_orders").limit(3))

print("=== bronze.raw_purchase_order_lines (sample) ===")
display(spark.table(f"{CATALOG}.bronze.raw_purchase_order_lines").limit(3))

# COMMAND ----------

print("\nBronze ingestion complete.")
print(f"  raw_suppliers            : {rows_suppliers} rows")
print(f"  raw_products             : {rows_products} rows")
print(f"  raw_purchase_orders      : {rows_po} rows")
print(f"  raw_purchase_order_lines : {rows_lines} rows")
print(f"\n  Physical storage         : {ADLS_BASE}/catalog/{CATALOG}/bronze/")
print("\nNext: Run nb_02_silver_cleanse.py")
