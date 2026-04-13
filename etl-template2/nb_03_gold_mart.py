# Databricks notebook source
# MAGIC %md
# MAGIC # nb_03_gold_mart
# MAGIC **Layer:** Gold
# MAGIC **Purpose:** Build three business-ready gold tables from `silver.po_lines_enriched`.
# MAGIC No joins required — silver already conformed everything.
# MAGIC
# MAGIC **Gold Tables Produced:**
# MAGIC | Table | Grain | Use Case |
# MAGIC |---|---|---|
# MAGIC | `gold.fct_po_lines` | One row per PO line | Detailed fact for ad-hoc analysis |
# MAGIC | `gold.rpt_supplier_performance` | One row per supplier | Supplier scorecard / KPI dashboard |
# MAGIC | `gold.rpt_spend_by_category` | One row per category × month | Spend analytics by category |

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

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG    = "supply_chain_dev"
BATCH_DATE = "2024-04-10"

# Note: gold Delta tables are written via saveAsTable — Unity Catalog stores them
# at: {ADLS_BASE}/catalog/{CATALOG}/gold/<table_name>/
# Storage is managed by ext_loc_sc_catalog registered in nb_00.

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F, Window

RUN_ID = str(uuid.uuid4())
SOURCE = f"{CATALOG}.silver.po_lines_enriched"
print(f"Catalog      : {CATALOG}")
print(f"Source Table : {SOURCE}")
print(f"Storage Base : {ADLS_BASE}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration
# MAGIC
# MAGIC | Setting | Value | Rationale |
# MAGIC |---|---|---|
# MAGIC | `adaptive.enabled` | true | AQE adapts window and aggregation plans at runtime |
# MAGIC | `coalescePartitions.enabled` | true | Merges small partitions after groupBy/window shuffles |
# MAGIC | `skewJoin.enabled` | true | Protects against data skew in category/supplier groupings |
# MAGIC | `shuffle.partitions` | 8 | Small dataset — prevents 200 nearly-empty shuffle tasks |
# MAGIC | `optimizeWrite` | true | Delta auto-packs Parquet files; avoids small-file problem |
# MAGIC | `autoCompact` | true | Delta compacts incrementally after each write |
# MAGIC | `windowExec.buffer.spill.threshold` | 4096 | Limits in-memory row buffer per window partition before spilling |

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                             "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                    "true")
spark.conf.set("spark.sql.shuffle.partitions",                           "8")   # tune up for larger datasets
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",           "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",             "true")
spark.conf.set("spark.sql.windowExec.buffer.spill.threshold",            "4096")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## 1. Read & Cache Silver Source
# MAGIC
# MAGIC `silver.po_lines_enriched` is the single source for all three gold tables.
# MAGIC Caching it avoids re-reading the Delta table from ADLS three separate times.
# MAGIC `silver.count()` is called immediately after `.cache()` to materialise the
# MAGIC cached plan on the cluster before any gold transforms start.

# COMMAND ----------

silver = spark.table(SOURCE)
silver.cache()
source_rows = silver.count()   # materialise cache now; subsequent reads are in-memory
print(f"Source rows: {source_rows}  (cached in memory)")
silver.printSchema()

# COMMAND ----------
# MAGIC %md ## 2. Gold Fact — `fct_po_lines`
# MAGIC
# MAGIC Adds surrogate key and final business measures. This is the detailed fact table —
# MAGIC every PO line, enriched and ready for slice-and-dice.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="supply_chain",
    entity_name="fct_po_lines", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    fct_po_lines = (silver

        .select(
            "line_id", "po_id", "supplier_id", "product_id",
            "supplier_name", "country", "region", "payment_terms", "rating",
            "product_name", "category", "sub_category", "uom",
            "po_date", "expected_delivery_date", "actual_delivery_date",
            "status", "currency", "shipping_cost",
            "is_on_time", "days_late",
            "line_status",
            "quantity_ordered", "quantity_received",
            "unit_cost", "unit_price",
            "ordered_value", "received_value", "receipt_rate_pct", "margin_pct",
        )

        # Date attributes for partition pruning and slicing
        .withColumn("po_year",    F.year("po_date"))
        .withColumn("po_month",   F.month("po_date"))
        .withColumn("po_quarter", F.quarter("po_date"))

        # Variance columns
        .withColumn("quantity_variance",
            F.col("quantity_received") - F.col("quantity_ordered"))
        .withColumn("value_variance",
            F.round(F.col("received_value") - F.col("ordered_value"), 2))

        # Surrogate key (MD5 is deterministic — safe for idempotent reruns)
        .withColumn("fct_line_sk", F.md5(F.col("line_id")))

        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    # Partition by po_year + po_month — gold queries almost always filter on date ranges.
    # This eliminates full table scans for monthly/quarterly dashboards.
    (fct_po_lines.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("po_year", "po_month")
        .saveAsTable(f"{CATALOG}.gold.fct_po_lines"))

    rows_fct = fct_po_lines.count()
    audit.set_rows_written(rows_fct)
    print(f"gold.fct_po_lines: {rows_fct} rows")

# ZORDER within each partition by the most-filtered columns for BI queries
spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`fct_po_lines`"
          f" ZORDER BY (supplier_id, category, line_status)")

# COMMAND ----------
# MAGIC %md ## 3. Report — `rpt_supplier_performance`
# MAGIC
# MAGIC One row per supplier summarising delivery performance, spend, and receipt rate.
# MAGIC This directly replaces an Alteryx Summarize workflow.
# MAGIC
# MAGIC **Performance note:** `groupBy` on `supplier_id` (10 unique values) produces very
# MAGIC few output partitions. AQE coalescePartitions will automatically reduce the 8
# MAGIC shuffle partitions down to match, preventing empty tasks.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="supply_chain",
    entity_name="rpt_supplier_performance", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    rpt_supplier = (fct_po_lines
        .filter(F.col("status") != "CANCELLED")
        .groupBy("supplier_id","supplier_name","country","region","rating","payment_terms")
        .agg(
            F.countDistinct("po_id")                                        .alias("total_pos"),
            F.count("line_id")                                              .alias("total_lines"),
            F.sum("ordered_value")                                          .alias("total_ordered_value"),
            F.sum("received_value")                                         .alias("total_received_value"),
            F.sum("quantity_ordered")                                       .alias("total_qty_ordered"),
            F.sum("quantity_received")                                      .alias("total_qty_received"),
            F.avg(F.when(F.col("is_on_time").isNotNull(),
                  F.col("is_on_time").cast("int")))                         .alias("on_time_rate"),
            F.avg(F.when(F.col("days_late").isNotNull(),
                  F.col("days_late")))                                      .alias("avg_days_late"),
            F.count(F.when(F.col("receipt_rate_pct") < 100, 1))            .alias("partial_deliveries"),
            F.min("po_date")                                                .alias("first_po_date"),
            F.max("po_date")                                                .alias("last_po_date"),
        )
        .withColumn("overall_receipt_rate_pct",
            F.when(F.col("total_qty_ordered") > 0,
                F.round(F.col("total_qty_received") / F.col("total_qty_ordered") * 100, 1)
            ).otherwise(0)
        )
        .withColumn("on_time_delivery_pct",
            F.round(F.col("on_time_rate") * 100, 1)
        )
        .withColumn("performance_score",
            F.round(
                (F.col("overall_receipt_rate_pct") + F.coalesce(F.col("on_time_delivery_pct"), F.lit(50))) / 40, 2
            )
        )
        # Window rank — single partition over a small number of suppliers, low shuffle cost
        .withColumn("spend_rank",
            F.rank().over(Window.orderBy(F.col("total_received_value").desc()))
        )
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
        .orderBy("spend_rank")
    )

    (rpt_supplier.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.rpt_supplier_performance"))

    rows_supplier = rpt_supplier.count()
    audit.set_rows_written(rows_supplier)
    print(f"gold.rpt_supplier_performance: {rows_supplier} rows")

# ZORDER by spend_rank and country for dashboard filter patterns
spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`rpt_supplier_performance`"
          f" ZORDER BY (spend_rank, country)")

# COMMAND ----------
# MAGIC %md ## 4. Report — `rpt_spend_by_category`
# MAGIC
# MAGIC Monthly spend breakdown by product category. Includes MoM spend change.
# MAGIC
# MAGIC **Performance note:** The `lag()` window function is partitioned by `category`
# MAGIC (only a few distinct values) so the window shuffle is small. Partitioning the
# MAGIC output Delta table by `po_year` + `po_month` means monthly dashboard queries
# MAGIC scan only the relevant partitions.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="supply_chain",
    entity_name="rpt_spend_by_category", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    monthly_category = (fct_po_lines
        .filter(F.col("line_status").isin("RECEIVED","PARTIAL"))
        .groupBy("po_year","po_month","category","sub_category")
        .agg(
            F.sum("received_value")        .alias("total_spend"),
            F.sum("quantity_received")     .alias("total_qty_received"),
            F.countDistinct("supplier_id") .alias("unique_suppliers"),
            F.countDistinct("product_id")  .alias("unique_products"),
            F.countDistinct("po_id")       .alias("unique_pos"),
            F.avg("unit_cost")             .alias("avg_unit_cost"),
        )
        .withColumn("month_label",
            F.concat(F.col("po_year").cast("string"), F.lit("-"),
                     F.lpad(F.col("po_month").cast("string"), 2, "0"))
        )
    )

    # Lag window — partitioned by category, ordered by year+month
    w_mom = Window.partitionBy("category").orderBy("po_year","po_month")

    rpt_category = (monthly_category
        .withColumn("prev_month_spend", F.lag("total_spend", 1).over(w_mom))
        .withColumn("mom_spend_change",
            F.when(
                F.col("prev_month_spend").isNotNull() & (F.col("prev_month_spend") > 0),
                F.round(
                    (F.col("total_spend") - F.col("prev_month_spend"))
                    / F.col("prev_month_spend") * 100, 1
                )
            ).otherwise(F.lit(None))
        )
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
        .orderBy("po_year","po_month","category")
    )

    # Partition by year + month matches typical dashboard query pattern
    (rpt_category.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("po_year","po_month")
        .saveAsTable(f"{CATALOG}.gold.rpt_spend_by_category"))

    rows_category = rpt_category.count()
    audit.set_rows_written(rows_category)
    print(f"gold.rpt_spend_by_category: {rows_category} rows")

# ZORDER within each partition by category for category-filter queries
spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`rpt_spend_by_category`"
          f" ZORDER BY (category, sub_category)")

# Release cached silver now that all three gold tables are built
silver.unpersist()
print("Silver cache released.")

# COMMAND ----------
# MAGIC %md ## 5. DQ Checks — Gold Fact

# COMMAND ----------

print("\n=== DQ: gold.fct_po_lines ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.gold.fct_po_lines"),
    suite=[
        {"name": "not_null__fct_line_sk",    "type": "not_null",            "column": "fct_line_sk",    "threshold": 1.0},
        {"name": "unique__fct_line_sk",      "type": "unique",              "column": "fct_line_sk",    "threshold": 1.0},
        {"name": "not_null__po_id",          "type": "not_null",            "column": "po_id",          "threshold": 1.0},
        {"name": "not_null__supplier_id",    "type": "not_null",            "column": "supplier_id",    "threshold": 1.0},
        {"name": "not_null__category",       "type": "not_null",            "column": "category",       "threshold": 1.0},
        {"name": "range__ordered_value",     "type": "range",               "column": "ordered_value",  "min": 0, "threshold": 1.0},
        {"name": "range__receipt_rate_pct",  "type": "range",               "column": "receipt_rate_pct","min": 0, "max": 100, "threshold": 1.0},
        {"name": "row_count_min_1",          "type": "row_count_threshold", "min_rows": 1,              "threshold": 1.0},
    ],
    entity_name="fct_po_lines", layer="gold",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 6. Results Preview

# COMMAND ----------

print("=== Supplier Performance ===")
display(spark.sql(f"""
    SELECT supplier_name, country, total_pos, total_ordered_value,
           overall_receipt_rate_pct, on_time_delivery_pct, avg_days_late, performance_score, spend_rank
    FROM {CATALOG}.gold.rpt_supplier_performance
    ORDER BY spend_rank
"""))

print("=== Spend by Category (monthly) ===")
display(spark.sql(f"""
    SELECT month_label, category, total_spend, total_qty_received,
           unique_suppliers, mom_spend_change
    FROM {CATALOG}.gold.rpt_spend_by_category
    ORDER BY month_label, total_spend DESC
"""))

print("=== Fact Table Sample ===")
display(spark.sql(f"""
    SELECT po_id, supplier_name, product_name, category,
           quantity_ordered, quantity_received, ordered_value, received_value,
           receipt_rate_pct, is_on_time, days_late
    FROM {CATALOG}.gold.fct_po_lines
    ORDER BY po_id, line_id
    LIMIT 15
"""))

# COMMAND ----------

print("\n" + "="*55)
print("GOLD MART COMPLETE")
print("="*55)
print(f"  fct_po_lines              : {rows_fct} rows")
print(f"  rpt_supplier_performance  : {rows_supplier} rows")
print(f"  rpt_spend_by_category     : {rows_category} rows")
print(f"  Physical storage          : {ADLS_BASE}/catalog/{CATALOG}/gold/")
print("="*55)
