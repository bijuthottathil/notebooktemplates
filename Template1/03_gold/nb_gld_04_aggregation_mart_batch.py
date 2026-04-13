# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_04_aggregation_mart_batch
# MAGIC **Layer:** Gold
# MAGIC **Pattern:** Rolling Aggregation Mart (Window Functions + Period-over-Period)
# MAGIC **Purpose:** Compute rolling metrics, period-over-period comparisons, and cumulative KPIs.
# MAGIC Designed for executive-level dashboards and KPI tracking.
# MAGIC
# MAGIC **Table:** `gold.agg_<subject>_<period>`
# MAGIC
# MAGIC **Metrics produced:**
# MAGIC - Rolling 7-day and 30-day revenue
# MAGIC - Month-over-month and year-over-year growth
# MAGIC - Running totals (YTD)
# MAGIC - Rank / percentile within peer group
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Executive dashboards requiring rolling windows
# MAGIC - KPI tracking with period comparisons
# MAGIC - Replacing Alteryx Running Total / Multi-Row Formula tools

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",    "company_dev",  "Catalog")
dbutils.widgets.text("batch_date", "",             "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env",    "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F, Window

CATALOG    = dbutils.widgets.get("catalog")
raw_bd     = dbutils.widgets.get("batch_date").strip()
BATCH_DATE = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID     = str(uuid.uuid4())

DAILY_RPT  = f"`{CATALOG}`.`gold`.`rpt_sales_daily`"
TGT_TABLE  = f"`{CATALOG}`.`gold`.`agg_sales_kpi_daily`"

print(f"Source : {DAILY_RPT}")
print(f"Target : {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 2. Read Daily Report (full history needed for rolling windows)

# COMMAND ----------

daily_df = (spark.table(DAILY_RPT)
    .select(
        "order_date", "order_year", "order_month", "order_week",
        "account_tier", "category", "country_name",
        "order_count", "total_net_revenue", "total_gross_profit",
        "total_units_sold", "unique_customers", "avg_gross_margin_pct",
        "return_rate"
    )
)
print(f"Daily rows loaded: {daily_df.count():,}")

# COMMAND ----------
# MAGIC %md ## 3. Rolling Windows (7-day, 30-day)

# COMMAND ----------

# Window by category × account_tier, ordered by date
w_7d  = (Window
    .partitionBy("category", "account_tier")
    .orderBy(F.col("order_date").cast("long"))
    .rangeBetween(-(6 * 86400), 0)       # 7-day rolling (unix seconds)
)
w_30d = (Window
    .partitionBy("category", "account_tier")
    .orderBy(F.col("order_date").cast("long"))
    .rangeBetween(-(29 * 86400), 0)      # 30-day rolling
)

# Cumulative window (YTD per partition)
w_ytd = (Window
    .partitionBy("category", "account_tier", "order_year")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# Overall date window for cross-segment rank
w_date = Window.partitionBy("order_date").orderBy(F.col("total_net_revenue").desc())

rolling_df = (daily_df
    .withColumn("revenue_rolling_7d",       F.sum("total_net_revenue").over(w_7d))
    .withColumn("revenue_rolling_30d",      F.sum("total_net_revenue").over(w_30d))
    .withColumn("orders_rolling_7d",        F.sum("order_count").over(w_7d))
    .withColumn("orders_rolling_30d",       F.sum("order_count").over(w_30d))
    .withColumn("revenue_ytd",              F.sum("total_net_revenue").over(w_ytd))
    .withColumn("orders_ytd",               F.sum("order_count").over(w_ytd))
    .withColumn("revenue_rank_on_date",     F.rank().over(w_date))
    .withColumn("revenue_pct_of_daily_total",
        F.round(
            F.col("total_net_revenue") /
            F.nullif(F.sum("total_net_revenue").over(Window.partitionBy("order_date")), F.lit(0)) * 100,
            2
        )
    )
)

# COMMAND ----------
# MAGIC %md ## 4. Period-over-Period (MoM, YoY)

# COMMAND ----------

# Lag windows for MoM and YoY comparisons
w_lag_mom = Window.partitionBy("category", "account_tier", "order_year").orderBy("order_month")
w_lag_yoy = Window.partitionBy("category", "account_tier", "order_month").orderBy("order_year")

# Monthly aggregates first, then join back
monthly_agg = (daily_df
    .groupBy("order_year", "order_month", "category", "account_tier")
    .agg(
        F.sum("total_net_revenue").alias("monthly_revenue"),
        F.sum("order_count").alias("monthly_orders"),
    )
    .withColumn("prev_month_revenue",   F.lag("monthly_revenue", 1).over(w_lag_mom))
    .withColumn("prev_year_revenue",    F.lag("monthly_revenue", 1).over(w_lag_yoy))
    .withColumn("mom_revenue_growth_pct",
        F.when(F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") != 0),
            F.round((F.col("monthly_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100, 2)
        ).otherwise(F.lit(None))
    )
    .withColumn("yoy_revenue_growth_pct",
        F.when(F.col("prev_year_revenue").isNotNull() & (F.col("prev_year_revenue") != 0),
            F.round((F.col("monthly_revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue") * 100, 2)
        ).otherwise(F.lit(None))
    )
)

# Join back to daily grain
agg_df = (rolling_df
    .join(
        monthly_agg.select(
            "order_year", "order_month", "category", "account_tier",
            "mom_revenue_growth_pct", "yoy_revenue_growth_pct"
        ),
        on=["order_year", "order_month", "category", "account_tier"],
        how="left"
    )
    .withColumn("_agg_ingest_id",  F.lit(RUN_ID))
    .withColumn("_agg_ingest_ts",  F.current_timestamp())
    .withColumn("_agg_batch_date", F.lit(BATCH_DATE).cast("date"))
)

rows_agg = agg_df.count()
print(f"Aggregation rows: {rows_agg:,}")

# COMMAND ----------
# MAGIC %md ## 5. Write to Gold

# COMMAND ----------

(agg_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.agg_sales_kpi_daily")
)
print(f"Written {rows_agg:,} rows → {TGT_TABLE}")

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (order_date, category, account_tier)")

# COMMAND ----------
# MAGIC %md ## 6. Quick Sanity Preview

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        order_date,
        category,
        account_tier,
        total_net_revenue,
        revenue_rolling_7d,
        revenue_rolling_30d,
        revenue_ytd,
        mom_revenue_growth_pct,
        yoy_revenue_growth_pct
    FROM {TGT_TABLE}
    WHERE order_date = '{BATCH_DATE}'
    ORDER BY category, account_tier
    LIMIT 20
"""))

dbutils.jobs.taskValues.set(key="gold_agg_table", value=f"{CATALOG}.gold.agg_sales_kpi_daily")
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)
dbutils.notebook.exit(f"SUCCESS|{rows_agg}")
