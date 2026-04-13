# Databricks notebook source
# MAGIC %md
# MAGIC # nb_02_silver_cleanse
# MAGIC **Layer:** Silver
# MAGIC **Purpose:** Read the four bronze tables, apply type casting, null handling, and basic
# MAGIC data quality checks. Join purchase orders with suppliers and products to produce
# MAGIC a conformed `silver.po_lines_enriched` table ready for gold aggregations.
# MAGIC
# MAGIC **Bronze → Silver:**
# MAGIC ```
# MAGIC bronze.raw_suppliers            →  silver.suppliers
# MAGIC bronze.raw_products             →  silver.products
# MAGIC bronze.raw_purchase_orders      →  silver.purchase_orders
# MAGIC bronze.raw_purchase_order_lines →  silver.purchase_order_lines
# MAGIC
# MAGIC silver.purchase_orders
# MAGIC   + silver.purchase_order_lines   →  silver.po_lines_enriched  (conformed join)
# MAGIC   + silver.suppliers              (broadcast — small dimension)
# MAGIC   + silver.products               (broadcast — small dimension)
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## Utilities

# COMMAND ----------

# MAGIC %run ./utilities/nb_utils_audit_logger

# COMMAND ----------

# MAGIC %run ./utilities/nb_utils_data_quality

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── ADLS Gen2 (must match nb_00) ─────────────────────────────────────────────
STORAGE_ACCOUNT = "dlscompanydev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template2"

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG    = "supply_chain_dev"
BATCH_DATE = "2024-04-10"

# Note: silver Delta tables are written via saveAsTable — Unity Catalog stores them
# at: {ADLS_BASE}/catalog/{CATALOG}/silver/<table_name>/
# No explicit path is needed here; storage is managed by the external location
# ext_loc_sc_catalog registered in nb_00.

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
print(f"Run ID       : {RUN_ID}")
print(f"Batch Date   : {BATCH_DATE}")
print(f"Catalog      : {CATALOG}")
print(f"Storage Base : {ADLS_BASE}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration
# MAGIC
# MAGIC | Setting | Value | Rationale |
# MAGIC |---|---|---|
# MAGIC | `adaptive.enabled` | true | AQE re-plans joins at runtime using actual row counts |
# MAGIC | `coalescePartitions.enabled` | true | AQE merges tiny partitions after shuffles |
# MAGIC | `skewJoin.enabled` | true | AQE splits skewed partitions in joins automatically |
# MAGIC | `shuffle.partitions` | 8 | This dataset is small; default 200 creates 200 tiny tasks |
# MAGIC | `autoBroadcastJoinThreshold` | 20 MB | Suppliers (~10 rows) and products (~15 rows) fit easily |
# MAGIC | `optimizeWrite` | true | Delta auto-sizes Parquet files on write — no small-file problem |
# MAGIC | `autoCompact` | true | Delta compacts after writes to keep file counts healthy |

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.shuffle.partitions",                        "8")    # tune up for larger datasets
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",                str(20 * 1024 * 1024))  # 20 MB
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",        "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",          "true")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## Helper — Drop Bronze Metadata Columns

# COMMAND ----------

def drop_meta(df):
    """Remove bronze internal metadata columns before silver write."""
    meta = [c for c in df.columns if c.startswith("_")]
    return df.drop(*meta)

# COMMAND ----------
# MAGIC %md ## 1. Cleanse Suppliers

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="supply_chain",
    entity_name="suppliers", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    suppliers_raw = spark.table(f"{CATALOG}.bronze.raw_suppliers")
    audit.set_rows_read(suppliers_raw.count())

    suppliers = (drop_meta(suppliers_raw)
        .withColumn("rating",          F.col("rating").cast("double"))
        .withColumn("is_active",       F.col("is_active").cast("boolean"))
        .withColumn("supplier_name",   F.trim(F.col("supplier_name")))
        .withColumn("country",         F.trim(F.col("country")))
        .withColumn("contact_email",   F.lower(F.trim(F.col("contact_email"))))
        .filter(F.col("supplier_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (suppliers.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.suppliers"))

    rows_suppliers = suppliers.count()
    audit.set_rows_written(rows_suppliers)
    print(f"silver.suppliers: {rows_suppliers} rows")

# ZORDER by supplier_id so downstream joins skip files efficiently
spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`suppliers` ZORDER BY (supplier_id)")

# DQ checks on silver.suppliers
print("\n=== DQ: silver.suppliers ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.suppliers"),
    suite=[
        {"name": "not_null__supplier_id",   "type": "not_null", "column": "supplier_id",   "threshold": 1.0},
        {"name": "unique__supplier_id",     "type": "unique",   "column": "supplier_id",   "threshold": 1.0},
        {"name": "not_null__supplier_name", "type": "not_null", "column": "supplier_name", "threshold": 1.0},
        {"name": "range__rating",           "type": "range",    "column": "rating", "min": 0, "max": 5, "threshold": 1.0},
        {"name": "valid_email",             "type": "regex",    "column": "contact_email",
         "pattern": r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", "threshold": 0.95},
    ],
    entity_name="suppliers", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 2. Cleanse Products

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="supply_chain",
    entity_name="products", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    products_raw = spark.table(f"{CATALOG}.bronze.raw_products")
    audit.set_rows_read(products_raw.count())

    products = (drop_meta(products_raw)
        .withColumn("unit_cost",      F.col("unit_cost").cast("double"))
        .withColumn("unit_price",     F.col("unit_price").cast("double"))
        .withColumn("lead_time_days", F.col("lead_time_days").cast("int"))
        .withColumn("reorder_point",  F.col("reorder_point").cast("int"))
        .withColumn("is_active",      F.col("is_active").cast("boolean"))
        .withColumn("product_name",   F.trim(F.col("product_name")))
        .withColumn("category",       F.trim(F.col("category")))
        .withColumn("sub_category",   F.trim(F.col("sub_category")))
        .withColumn("margin_pct",
            F.when(F.col("unit_price") > 0,
                F.round((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100, 1)
            ).otherwise(F.lit(0.0))
        )
        .filter(F.col("product_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (products.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.products"))

    rows_products = products.count()
    audit.set_rows_written(rows_products)
    print(f"silver.products: {rows_products} rows")

# ZORDER by product_id and category for join + filter pushdown in gold
spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`products` ZORDER BY (product_id, category)")

# DQ checks on silver.products
print("\n=== DQ: silver.products ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.products"),
    suite=[
        {"name": "not_null__product_id",   "type": "not_null", "column": "product_id",   "threshold": 1.0},
        {"name": "unique__product_id",     "type": "unique",   "column": "product_id",   "threshold": 1.0},
        {"name": "not_null__product_name", "type": "not_null", "column": "product_name", "threshold": 1.0},
        {"name": "range__unit_cost",       "type": "range",    "column": "unit_cost",    "min": 0, "threshold": 1.0},
        {"name": "range__unit_price",      "type": "range",    "column": "unit_price",   "min": 0, "threshold": 1.0},
        {"name": "range__margin_pct",      "type": "range",    "column": "margin_pct",   "min": 0, "max": 100, "threshold": 1.0},
    ],
    entity_name="products", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 3. Cleanse Purchase Orders

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="supply_chain",
    entity_name="purchase_orders", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    po_raw = spark.table(f"{CATALOG}.bronze.raw_purchase_orders")
    audit.set_rows_read(po_raw.count())

    purchase_orders = (drop_meta(po_raw)
        .withColumn("po_date",                F.col("po_date").cast("date"))
        .withColumn("expected_delivery_date", F.col("expected_delivery_date").cast("date"))
        .withColumn("actual_delivery_date",   F.col("actual_delivery_date").cast("date"))
        .withColumn("shipping_cost",          F.col("shipping_cost").cast("double"))
        .withColumn("status",                 F.upper(F.trim(F.col("status"))))
        .withColumn("currency",               F.upper(F.trim(F.col("currency"))))
        .withColumn("notes",
            F.when(F.trim(F.col("notes")) == "", F.lit(None)).otherwise(F.col("notes")))
        .withColumn("is_on_time",
            F.when(F.col("actual_delivery_date").isNotNull(),
                F.col("actual_delivery_date") <= F.col("expected_delivery_date")
            ).otherwise(F.lit(None))
        )
        .withColumn("days_late",
            F.when(F.col("actual_delivery_date").isNotNull(),
                F.datediff(F.col("actual_delivery_date"), F.col("expected_delivery_date"))
            ).otherwise(F.lit(None))
        )
        .filter(F.col("po_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (purchase_orders.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.purchase_orders"))

    rows_po = purchase_orders.count()
    audit.set_rows_written(rows_po)
    print(f"silver.purchase_orders: {rows_po} rows")

# ZORDER by supplier_id + po_date for join and date-range filter pushdown
spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`purchase_orders` ZORDER BY (supplier_id, po_date)")

# DQ checks on silver.purchase_orders
print("\n=== DQ: silver.purchase_orders ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.purchase_orders"),
    suite=[
        {"name": "not_null__po_id",       "type": "not_null",        "column": "po_id",        "threshold": 1.0},
        {"name": "unique__po_id",         "type": "unique",          "column": "po_id",        "threshold": 1.0},
        {"name": "not_null__supplier_id", "type": "not_null",        "column": "supplier_id",  "threshold": 1.0},
        {"name": "valid_status",          "type": "accepted_values", "column": "status",
         "values": ["DELIVERED","CANCELLED","IN_TRANSIT","APPROVED","PENDING"],                 "threshold": 0.99},
        {"name": "range__shipping_cost",  "type": "range",           "column": "shipping_cost","min": 0, "threshold": 1.0},
    ],
    entity_name="purchase_orders", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 4. Cleanse Purchase Order Lines

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="supply_chain",
    entity_name="purchase_order_lines", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    lines_raw = spark.table(f"{CATALOG}.bronze.raw_purchase_order_lines")
    audit.set_rows_read(lines_raw.count())

    po_lines = (drop_meta(lines_raw)
        .withColumn("quantity_ordered",  F.col("quantity_ordered").cast("int"))
        .withColumn("quantity_received", F.col("quantity_received").cast("int"))
        .withColumn("unit_cost",         F.col("unit_cost").cast("double"))
        .withColumn("line_status",       F.upper(F.trim(F.col("line_status"))))
        .withColumn("ordered_value",  F.round(F.col("quantity_ordered")  * F.col("unit_cost"), 2))
        .withColumn("received_value", F.round(F.col("quantity_received") * F.col("unit_cost"), 2))
        .withColumn("receipt_rate_pct",
            F.when(F.col("quantity_ordered") > 0,
                F.round(F.col("quantity_received") / F.col("quantity_ordered") * 100, 1)
            ).otherwise(F.lit(0.0))
        )
        .filter(F.col("line_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (po_lines.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.purchase_order_lines"))

    rows_lines = po_lines.count()
    audit.set_rows_written(rows_lines)
    print(f"silver.purchase_order_lines: {rows_lines} rows")

# ZORDER by po_id + product_id — primary join keys for the enriched join below
spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`purchase_order_lines` ZORDER BY (po_id, product_id)")

# DQ checks on silver.purchase_order_lines
print("\n=== DQ: silver.purchase_order_lines ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.purchase_order_lines"),
    suite=[
        {"name": "not_null__line_id",        "type": "not_null",        "column": "line_id",          "threshold": 1.0},
        {"name": "unique__line_id",          "type": "unique",          "column": "line_id",          "threshold": 1.0},
        {"name": "not_null__po_id",          "type": "not_null",        "column": "po_id",            "threshold": 1.0},
        {"name": "not_null__product_id",     "type": "not_null",        "column": "product_id",       "threshold": 1.0},
        {"name": "range__quantity_ordered",  "type": "range",           "column": "quantity_ordered", "min": 0, "threshold": 1.0},
        {"name": "range__quantity_received", "type": "range",           "column": "quantity_received","min": 0, "threshold": 1.0},
        {"name": "range__receipt_rate_pct",  "type": "range",           "column": "receipt_rate_pct", "min": 0, "max": 100, "threshold": 1.0},
        {"name": "valid_line_status",        "type": "accepted_values", "column": "line_status",
         "values": ["RECEIVED","PARTIAL","CANCELLED","PENDING"],                                       "threshold": 0.99},
    ],
    entity_name="purchase_order_lines", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 5. Build Conformed Join — `po_lines_enriched`
# MAGIC
# MAGIC Join all four silver tables into a single wide table that the gold layer can
# MAGIC aggregate without any further joins.
# MAGIC
# MAGIC **Performance notes:**
# MAGIC - `suppliers` and `products` are small dimension tables — wrapped in `F.broadcast()`
# MAGIC   so each executor gets a local copy and no shuffle is needed for these joins.
# MAGIC - `po_lines_enriched` is `.cache()`-d before write so the `count()` call reuses
# MAGIC   the in-memory plan rather than recomputing the four-way join from scratch.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="supply_chain",
    entity_name="po_lines_enriched", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    # Re-read from silver (picks up cleansed types, benefits from ZORDER)
    sv_po        = spark.table(f"{CATALOG}.silver.purchase_orders")
    sv_lines     = spark.table(f"{CATALOG}.silver.purchase_order_lines")
    sv_suppliers = spark.table(f"{CATALOG}.silver.suppliers").drop("_silver_ts","_silver_batch_date")
    sv_products  = spark.table(f"{CATALOG}.silver.products").drop("_silver_ts","_silver_batch_date")

    audit.set_rows_read(sv_lines.count())

    po_lines_enriched = (sv_lines
        # Large fact → large fact join (shuffle join, benefits from ZORDER on po_id)
        .join(sv_po.select(
            "po_id","supplier_id","po_date","expected_delivery_date","actual_delivery_date",
            "status","currency","shipping_cost","is_on_time","days_late"
        ), on="po_id", how="left")

        # Small dimension — broadcast to avoid shuffle entirely (~10 rows)
        .join(F.broadcast(sv_suppliers.select(
            "supplier_id","supplier_name","country","region","payment_terms","rating"
        )), on="supplier_id", how="left")

        # Small dimension — broadcast to avoid shuffle entirely (~15 rows)
        .join(F.broadcast(sv_products.select(
            "product_id","product_name","category","sub_category","uom","unit_price","margin_pct"
        )), on="product_id", how="left")

        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    # Cache before write so count() reuses the materialised plan
    po_lines_enriched.cache()

    (po_lines_enriched.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.po_lines_enriched"))

    rows_enriched = po_lines_enriched.count()   # served from cache
    po_lines_enriched.unpersist()               # release memory
    audit.set_rows_written(rows_enriched)
    print(f"silver.po_lines_enriched: {rows_enriched} rows  (grain: one row per PO line)")

# ZORDER by the gold layer's primary grouping keys for partition pruning
spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`po_lines_enriched`"
          f" ZORDER BY (supplier_id, product_id, po_date)")

# COMMAND ----------
# MAGIC %md ## 6. Data Quality Summary — `po_lines_enriched`

# COMMAND ----------

enriched_df = spark.table(f"{CATALOG}.silver.po_lines_enriched")

print("\n=== DQ: silver.po_lines_enriched ===")
run_dq_suite(
    df=enriched_df,
    suite=[
        {"name": "not_null__line_id",     "type": "not_null", "column": "line_id",      "threshold": 1.0},
        {"name": "not_null__po_id",       "type": "not_null", "column": "po_id",        "threshold": 1.0},
        {"name": "not_null__supplier_id", "type": "not_null", "column": "supplier_id",  "threshold": 1.0},
        {"name": "not_null__product_id",  "type": "not_null", "column": "product_id",   "threshold": 1.0},
        {"name": "join_supplier_match",   "type": "not_null", "column": "supplier_name","threshold": 0.99},
        {"name": "join_product_match",    "type": "not_null", "column": "product_name", "threshold": 0.99},
        {"name": "row_count_min_1",       "type": "row_count_threshold", "min_rows": 1, "threshold": 1.0},
    ],
    entity_name="po_lines_enriched", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

display(enriched_df.select(
    "line_id","po_id","supplier_name","product_name","category",
    "quantity_ordered","quantity_received","unit_cost","ordered_value","receipt_rate_pct","line_status"
).orderBy("po_id","line_id").limit(10))

# COMMAND ----------

print(f"\nSilver cleanse complete.")
print(f"  silver.suppliers            : {rows_suppliers} rows")
print(f"  silver.products             : {rows_products} rows")
print(f"  silver.purchase_orders      : {rows_po} rows")
print(f"  silver.purchase_order_lines : {rows_lines} rows")
print(f"  silver.po_lines_enriched    : {rows_enriched} rows")
print(f"  Physical storage : {ADLS_BASE}/catalog/{CATALOG}/silver/")
print("  Next: Run nb_03_gold_mart.py")
