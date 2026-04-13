# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_01_fact_table_batch
# MAGIC **Layer:** Gold | **Pattern:** Fact Table Build
# MAGIC **Purpose:** Join conformed silver entities into a business-grain fact table with measures.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG      = "company_dev"
FACT_NAME    = "sales_orders"
LOOKBACK     = 1                # Reprocess N days back to catch late arrivals
BATCH_DATE   = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F

RUN_ID     = str(uuid.uuid4())
START_DATE = str(datetime.date.fromisoformat(BATCH_DATE) - datetime.timedelta(days=LOOKBACK))
TGT_TABLE  = f"`{CATALOG}`.`gold`.`fct_{FACT_NAME}`"

print(f"Target Table  : {TGT_TABLE}")
print(f"Date Window   : {START_DATE} → {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 1. Read Silver Sources

# COMMAND ----------

orders_df = (spark.table(f"`{CATALOG}`.`silver`.`supply_chain__orders`")
    .filter(F.col("_silver_batch_date").between(
        F.lit(START_DATE).cast("date"), F.lit(BATCH_DATE).cast("date")
    ))
    .select("order_id", "customer_id", "product_id", "order_date",
            "quantity", "unit_price", "discount_pct", "status",
            "warehouse_id", "ship_date", "delivery_date")
)
print(f"Orders: {orders_df.count():,}")

customers_df = (spark.table(f"`{CATALOG}`.`silver`.`conformed__customer_360`")
    .filter(F.col("_silver_batch_date") == F.lit(BATCH_DATE).cast("date"))
    .select("customer_id", "account_tier", "segment_code", "country")
)

products_df = (spark.table(f"`{CATALOG}`.`silver`.`erp__products`")
    .filter(F.col("_is_current") == True)
    .select("product_id", "product_name", "category", "sub_category", "brand", "cost_price")
)

# COMMAND ----------
# MAGIC %md ## 2. Build Fact Table

# COMMAND ----------

fact_df = (orders_df
    .join(customers_df, on="customer_id", how="left")
    .join(products_df,  on="product_id",  how="left")

    # Measures
    .withColumn("gross_revenue",   F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("discount_amount", F.round(F.col("gross_revenue") * F.col("discount_pct") / 100, 2))
    .withColumn("net_revenue",     F.round(F.col("gross_revenue") - F.col("discount_amount"), 2))
    .withColumn("cogs",            F.round(F.col("quantity") * F.coalesce(F.col("cost_price"), F.lit(0.0)), 2))
    .withColumn("gross_profit",    F.round(F.col("net_revenue") - F.col("cogs"), 2))
    .withColumn("gross_margin_pct",
        F.when(F.col("net_revenue") != 0,
            F.round(F.col("gross_profit") / F.col("net_revenue") * 100, 2)
        ).otherwise(F.lit(0.0))
    )

    # Date attributes
    .withColumn("order_year",      F.year(F.col("order_date")))
    .withColumn("order_month",     F.month(F.col("order_date")))
    .withColumn("order_quarter",   F.quarter(F.col("order_date")))
    .withColumn("order_week",      F.weekofyear(F.col("order_date")))
    .withColumn("is_weekend_order",F.dayofweek(F.col("order_date")).isin([1, 7]))
    .withColumn("days_to_ship",    F.datediff(F.col("ship_date"), F.col("order_date")))
    .withColumn("days_to_deliver", F.datediff(F.col("delivery_date"), F.col("order_date")))

    # Surrogate key
    .withColumn("fct_order_sk",    F.md5(F.concat_ws("|", F.col("order_id"), F.lit(BATCH_DATE))))

    # Metadata
    .withColumn("_gold_ingest_id",  F.lit(RUN_ID))
    .withColumn("_gold_ingest_ts",  F.current_timestamp())
    .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
)

rows_fact = fact_df.count()
print(f"Fact rows: {rows_fact:,}")

# COMMAND ----------
# MAGIC %md ## 3. Write to Gold & Optimize

# COMMAND ----------

(fact_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_gold_batch_date BETWEEN '{START_DATE}' AND '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.fct_{FACT_NAME}")
)

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (order_date, customer_id, product_id)")
print(f"Written {rows_fact:,} rows → {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Sanity Check

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_orders, COUNT(DISTINCT customer_id) AS unique_customers,
           SUM(net_revenue) AS total_net_revenue, AVG(gross_margin_pct) AS avg_margin_pct,
           MIN(order_date) AS min_date, MAX(order_date) AS max_date
    FROM {TGT_TABLE}
    WHERE _gold_batch_date = '{BATCH_DATE}'
"""))
