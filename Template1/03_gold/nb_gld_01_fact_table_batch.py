# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_01_fact_table_batch
# MAGIC **Layer:** Gold
# MAGIC **Pattern:** Fact Table Build (Grain: one row per transactional event)
# MAGIC **Purpose:** Build a business-grain fact table by joining conformed silver entities.
# MAGIC Applies business measures, surrogate keys, and conformed dimensions.
# MAGIC
# MAGIC **Naming Convention** *(Netflix / Data Vault inspired)*:
# MAGIC ```
# MAGIC Notebook : nb_gld_<seq>_<type>_<domain>
# MAGIC Table     : <catalog>.gold.fct_<domain>_<subject>
# MAGIC ```
# MAGIC
# MAGIC **Output Table:** `gold.fct_sales_orders`
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Building a measurable fact table (sales, orders, events, transactions)
# MAGIC - Replacing Alteryx summarize/formula workflows that produced BI-ready outputs
# MAGIC - Source: conformed silver layer after SCD2 / merge pattern

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",        "company_dev",    "Catalog")
dbutils.widgets.text("fact_name",      "sales_orders",   "Fact Table Name (suffix of fct_)")
dbutils.widgets.text("batch_date",     "",               "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_days",  "1",              "Lookback Days (reprocess N days to handle late arrivals)")
dbutils.widgets.dropdown("env",        "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG       = dbutils.widgets.get("catalog")
FACT_NAME     = dbutils.widgets.get("fact_name")
LOOKBACK      = int(dbutils.widgets.get("lookback_days"))

raw_bd        = dbutils.widgets.get("batch_date").strip()
BATCH_DATE    = raw_bd if raw_bd else str(datetime.date.today())
START_DATE    = str(datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=LOOKBACK))
RUN_ID        = str(uuid.uuid4())

TGT_TABLE     = f"`{CATALOG}`.`gold`.`fct_{FACT_NAME}`"

print(f"Target Table  : {TGT_TABLE}")
print(f"Batch Date    : {BATCH_DATE}")
print(f"Lookback Start: {START_DATE}")

# COMMAND ----------
# MAGIC %md ## 2. Read Silver Sources

# COMMAND ----------

# ── Orders (spine of the fact) ────────────────────────────────────────────────
orders_df = (spark.table(f"`{CATALOG}`.`silver`.`supply_chain__orders`")
    .filter(F.col("_silver_batch_date").between(
        F.lit(START_DATE).cast("date"),
        F.lit(BATCH_DATE).cast("date")
    ))
    .select(
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "unit_price", "discount_pct", "status",
        "warehouse_id", "ship_date", "delivery_date"
    )
)
print(f"Orders: {orders_df.count():,}")

# ── Customer dimension (current snapshot) ────────────────────────────────────
customers_df = (spark.table(f"`{CATALOG}`.`silver`.`conformed__customer_360`")
    .filter(F.col("_silver_batch_date") == F.lit(BATCH_DATE).cast("date"))
    .select("customer_id", "account_tier", "segment_code", "country")
)

# ── Product dimension ─────────────────────────────────────────────────────────
products_df = (spark.table(f"`{CATALOG}`.`silver`.`erp__products`")
    .filter(F.col("_is_current") == True)
    .select("product_id", "product_name", "category", "sub_category", "brand", "cost_price")
)

# COMMAND ----------
# MAGIC %md ## 3. Build Fact Table

# COMMAND ----------

fact_df = (orders_df

    # ── Join dimensions ───────────────────────────────────────────────────────
    .join(customers_df, on="customer_id", how="left")
    .join(products_df,  on="product_id",  how="left")

    # ── Business Measures ─────────────────────────────────────────────────────

    # Gross revenue
    .withColumn("gross_revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    # Discount amount
    .withColumn("discount_amount",
        F.round(F.col("gross_revenue") * F.col("discount_pct") / 100, 2)
    )
    # Net revenue
    .withColumn("net_revenue",
        F.round(F.col("gross_revenue") - F.col("discount_amount"), 2)
    )
    # Gross profit (requires product cost)
    .withColumn("cogs",
        F.round(F.col("quantity") * F.coalesce(F.col("cost_price"), F.lit(0.0)), 2)
    )
    .withColumn("gross_profit",
        F.round(F.col("net_revenue") - F.col("cogs"), 2)
    )
    .withColumn("gross_margin_pct",
        F.when(F.col("net_revenue") != 0,
            F.round(F.col("gross_profit") / F.col("net_revenue") * 100, 2)
        ).otherwise(F.lit(0.0))
    )

    # ── Date Dimensions ───────────────────────────────────────────────────────
    .withColumn("order_year",    F.year(F.col("order_date")))
    .withColumn("order_month",   F.month(F.col("order_date")))
    .withColumn("order_quarter", F.quarter(F.col("order_date")))
    .withColumn("order_week",    F.weekofyear(F.col("order_date")))
    .withColumn("is_weekend_order",
        F.dayofweek(F.col("order_date")).isin([1, 7])
    )
    # Days to ship / days to deliver
    .withColumn("days_to_ship",
        F.datediff(F.col("ship_date"), F.col("order_date"))
    )
    .withColumn("days_to_deliver",
        F.datediff(F.col("delivery_date"), F.col("order_date"))
    )

    # ── Surrogate Key (stable across reruns) ──────────────────────────────────
    .withColumn("fct_order_sk",
        F.md5(F.concat_ws("|", F.col("order_id"), F.lit(BATCH_DATE)))
    )

    # ── Gold metadata ─────────────────────────────────────────────────────────
    .withColumn("_gold_ingest_id",  F.lit(RUN_ID))
    .withColumn("_gold_ingest_ts",  F.current_timestamp())
    .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
)

rows_fact = fact_df.count()
print(f"Fact rows: {rows_fact:,}")

# COMMAND ----------
# MAGIC %md ## 4. Write to Gold (Replace Lookback Window)

# COMMAND ----------

(fact_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere",
        f"_gold_batch_date BETWEEN '{START_DATE}' AND '{BATCH_DATE}'"
    )
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.fct_{FACT_NAME}")
)

print(f"Written {rows_fact:,} rows → {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Optimize for BI Query Patterns

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE {TGT_TABLE}
    ZORDER BY (order_date, customer_id, product_id)
""")
print("OPTIMIZE + ZORDER applied.")

# COMMAND ----------
# MAGIC %md ## 6. Compute Quick Sanity Metrics

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)                        AS total_orders,
        COUNT(DISTINCT customer_id)     AS unique_customers,
        COUNT(DISTINCT product_id)      AS unique_products,
        SUM(net_revenue)                AS total_net_revenue,
        AVG(gross_margin_pct)           AS avg_gross_margin_pct,
        MIN(order_date)                 AS min_order_date,
        MAX(order_date)                 AS max_order_date
    FROM {TGT_TABLE}
    WHERE _gold_batch_date = '{BATCH_DATE}'
""").show()

# COMMAND ----------
# MAGIC %md ## 7. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="gold_table",   value=f"{CATALOG}.gold.fct_{FACT_NAME}")
dbutils.jobs.taskValues.set(key="rows_written", value=rows_fact)
dbutils.jobs.taskValues.set(key="run_id",       value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_fact}")
