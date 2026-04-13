# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_03_report_mart_batch
# MAGIC **Layer:** Gold
# MAGIC **Pattern:** Pre-Aggregated Report Mart
# MAGIC **Purpose:** Build pre-aggregated summary tables optimised for BI tools (Power BI, Tableau,
# MAGIC Looker). Pushes computation to batch so dashboards are fast. Replace Alteryx
# MAGIC Summarize tool chains that produced flat aggregate exports.
# MAGIC
# MAGIC **Table:** `gold.rpt_<subject>_<grain>`
# MAGIC
# MAGIC **Grains produced by this notebook:**
# MAGIC ```
# MAGIC rpt_sales_daily          ← one row per day × product_category × account_tier
# MAGIC rpt_sales_monthly        ← one row per month × country × segment
# MAGIC rpt_customer_activity    ← one row per customer per month
# MAGIC ```
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Pre-aggregating data for BI tools with strict query SLAs
# MAGIC - Building semantic layer summary tables in the gold schema
# MAGIC - Replacing Alteryx Summarize → export-to-Excel/CSV workflows

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",          "company_dev",   "Catalog")
dbutils.widgets.text("batch_date",       "",              "Batch Date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_months",  "3",             "Months to recompute (rolling window)")
dbutils.widgets.dropdown("env",          "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta

CATALOG          = dbutils.widgets.get("catalog")
LOOKBACK_MONTHS  = int(dbutils.widgets.get("lookback_months"))

raw_bd           = dbutils.widgets.get("batch_date").strip()
BATCH_DATE       = raw_bd if raw_bd else str(datetime.date.today())
batch_dt         = datetime.date.fromisoformat(BATCH_DATE)
START_DATE       = str(batch_dt - relativedelta(months=LOOKBACK_MONTHS))
RUN_ID           = str(uuid.uuid4())

FACT_TABLE       = f"`{CATALOG}`.`gold`.`fct_sales_orders`"
DIM_CUSTOMER     = f"`{CATALOG}`.`gold`.`dim_customer`"

print(f"Fact Table   : {FACT_TABLE}")
print(f"Date Window  : {START_DATE} → {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 2. Read Gold Fact (Rolling Window)

# COMMAND ----------

fact_df = (spark.table(FACT_TABLE)
    .filter(F.col("order_date").between(
        F.lit(START_DATE).cast("date"),
        F.lit(BATCH_DATE).cast("date")
    ))
    .join(spark.table(DIM_CUSTOMER).select("customer_id", "country_name", "account_tier", "segment_code"),
          on="customer_id", how="left")
)
print(f"Fact rows in window: {fact_df.count():,}")

# COMMAND ----------
# MAGIC %md ## 3. Daily Sales Report (`rpt_sales_daily`)

# COMMAND ----------

daily_rpt = (fact_df
    .groupBy(
        "order_date",
        "order_year",
        "order_month",
        "order_week",
        "category",
        "sub_category",
        "account_tier",
        "country_name"
    )
    .agg(
        F.count("order_id")                         .alias("order_count"),
        F.countDistinct("customer_id")               .alias("unique_customers"),
        F.countDistinct("product_id")                .alias("unique_products"),
        F.sum("quantity")                            .alias("total_units_sold"),
        F.sum("gross_revenue")                       .alias("total_gross_revenue"),
        F.sum("discount_amount")                     .alias("total_discount_amount"),
        F.sum("net_revenue")                         .alias("total_net_revenue"),
        F.sum("gross_profit")                        .alias("total_gross_profit"),
        F.avg("gross_margin_pct")                    .alias("avg_gross_margin_pct"),
        F.avg("days_to_ship")                        .alias("avg_days_to_ship"),
        F.avg("days_to_deliver")                     .alias("avg_days_to_deliver"),
        F.sum(F.when(F.col("status") == "RETURNED",  F.col("net_revenue")).otherwise(0))
                                                     .alias("return_revenue"),
        F.count(F.when(F.col("status") == "RETURNED", 1))
                                                     .alias("return_count"),
    )
    .withColumn("return_rate",
        F.when(F.col("order_count") > 0,
            F.round(F.col("return_count") / F.col("order_count") * 100, 2)
        ).otherwise(0)
    )
    .withColumn("_rpt_ingest_id",  F.lit(RUN_ID))
    .withColumn("_rpt_ingest_ts",  F.current_timestamp())
    .withColumn("_rpt_batch_date", F.lit(BATCH_DATE).cast("date"))
)

(daily_rpt.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date >= '{START_DATE}' AND order_date <= '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.rpt_sales_daily")
)
print(f"rpt_sales_daily: {daily_rpt.count():,} rows written")

# COMMAND ----------
# MAGIC %md ## 4. Monthly Sales Report (`rpt_sales_monthly`)

# COMMAND ----------

monthly_rpt = (fact_df
    .groupBy(
        "order_year",
        "order_month",
        "account_tier",
        "segment_code",
        "country_name",
        "category"
    )
    .agg(
        F.count("order_id")                     .alias("order_count"),
        F.countDistinct("customer_id")           .alias("unique_customers"),
        F.sum("net_revenue")                     .alias("total_net_revenue"),
        F.sum("gross_profit")                    .alias("total_gross_profit"),
        F.avg("gross_margin_pct")                .alias("avg_gross_margin_pct"),
        F.sum("quantity")                        .alias("total_units_sold"),
        F.sum("discount_amount")                 .alias("total_discount_amount"),
    )
    .withColumn("revenue_per_customer",
        F.when(F.col("unique_customers") > 0,
            F.round(F.col("total_net_revenue") / F.col("unique_customers"), 2)
        ).otherwise(0)
    )
    .withColumn("month_label",
        F.concat(F.col("order_year").cast("string"), F.lit("-"),
                 F.lpad(F.col("order_month").cast("string"), 2, "0"))
    )
    .withColumn("_rpt_ingest_id",  F.lit(RUN_ID))
    .withColumn("_rpt_ingest_ts",  F.current_timestamp())
    .withColumn("_rpt_batch_date", F.lit(BATCH_DATE).cast("date"))
)

(monthly_rpt.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_year >= {batch_dt.year - 1}")
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.rpt_sales_monthly")
)
print(f"rpt_sales_monthly: {monthly_rpt.count():,} rows written")

# COMMAND ----------
# MAGIC %md ## 5. Customer Activity Report (`rpt_customer_activity`)

# COMMAND ----------

customer_activity = (fact_df
    .groupBy(
        "customer_id",
        "account_tier",
        "segment_code",
        "country_name",
        "order_year",
        "order_month",
    )
    .agg(
        F.count("order_id")                      .alias("orders_placed"),
        F.sum("net_revenue")                     .alias("net_revenue"),
        F.sum("quantity")                        .alias("units_purchased"),
        F.min("order_date")                      .alias("first_order_date"),
        F.max("order_date")                      .alias("last_order_date"),
        F.countDistinct("category")              .alias("categories_purchased"),
        F.avg("gross_margin_pct")                .alias("avg_margin"),
    )
    .withColumn("days_between_orders",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))
    )
    .withColumn("avg_order_value",
        F.when(F.col("orders_placed") > 0,
            F.round(F.col("net_revenue") / F.col("orders_placed"), 2)
        ).otherwise(0)
    )
    .withColumn("_rpt_ingest_id",  F.lit(RUN_ID))
    .withColumn("_rpt_ingest_ts",  F.current_timestamp())
    .withColumn("_rpt_batch_date", F.lit(BATCH_DATE).cast("date"))
)

(customer_activity.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere",
        f"order_year = {batch_dt.year} AND order_month IN ({batch_dt.month}, {(batch_dt.month - 1) or 12})"
    )
    .option("mergeSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable(f"{CATALOG}.gold.rpt_customer_activity")
)
print(f"rpt_customer_activity: {customer_activity.count():,} rows written")

# COMMAND ----------
# MAGIC %md ## 6. Optimize All Report Tables

# COMMAND ----------

for tbl, zcols in [
    (f"`{CATALOG}`.`gold`.`rpt_sales_daily`",        "order_date, category, account_tier"),
    (f"`{CATALOG}`.`gold`.`rpt_sales_monthly`",      "order_year, order_month, account_tier"),
    (f"`{CATALOG}`.`gold`.`rpt_customer_activity`",  "customer_id, order_year, order_month"),
]:
    spark.sql(f"OPTIMIZE {tbl} ZORDER BY ({zcols})")
    print(f"  OPTIMIZE applied: {tbl}")

# COMMAND ----------
# MAGIC %md ## 7. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="rpt_daily_table",    value=f"{CATALOG}.gold.rpt_sales_daily")
dbutils.jobs.taskValues.set(key="rpt_monthly_table",  value=f"{CATALOG}.gold.rpt_sales_monthly")
dbutils.jobs.taskValues.set(key="rpt_customer_table", value=f"{CATALOG}.gold.rpt_customer_activity")
dbutils.jobs.taskValues.set(key="run_id",             value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{BATCH_DATE}")
