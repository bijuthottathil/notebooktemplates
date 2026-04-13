# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_02_dimension_table_batch
# MAGIC **Layer:** Gold | **Pattern:** Conformed Dimension Table
# MAGIC **Purpose:** Publish a clean, BI-ready dimension table from the silver layer.
# MAGIC Applies business-friendly naming, surrogate keys, and derived attributes.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG      = "company_dev"
DIM_NAME     = "customer"
SOURCE_TABLE = "conformed__customer_360"   # Silver table name (no catalog/schema prefix)
NAT_KEY      = "customer_id"
BATCH_DATE   = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F

RUN_ID    = str(uuid.uuid4())
SRC_TABLE = f"`{CATALOG}`.`silver`.`{SOURCE_TABLE}`"
TGT_TABLE = f"`{CATALOG}`.`gold`.`dim_{DIM_NAME}`"

print(f"Source : {SRC_TABLE}")
print(f"Target : {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 1. Read Silver

# COMMAND ----------

silver_df = (spark.table(SRC_TABLE)
    .filter(F.col("_silver_batch_date") == F.lit(BATCH_DATE).cast("date"))
    .filter(F.col(NAT_KEY).isNotNull())
)
print(f"Silver rows: {silver_df.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Select & Rename to Business-Friendly Names

# COMMAND ----------

dim_df = silver_df.select(
    F.col("customer_id"),
    F.col("customer_name")     .alias("customer_full_name"),
    F.col("email")             .alias("email_address"),
    F.col("phone")             .alias("phone_number"),
    F.col("country")           .alias("country_name"),
    F.col("city")              .alias("city_name"),
    F.col("account_tier"),
    F.col("account_type"),
    F.col("account_status"),
    F.col("payment_terms"),
    F.col("credit_limit"),
    F.col("segment_code"),
    F.col("segment_label"),
    F.col("ltv_band"),
    F.col("churn_risk_score"),
    F.col("is_high_value"),
    F.col("customer_age_days"),
    F.col("created_at")        .alias("customer_since_ts"),
)

# COMMAND ----------
# MAGIC %md ## 3. Add Derived Business Attributes

# COMMAND ----------

dim_enriched = (dim_df
    .withColumn("customer_tenure_band",
        F.when(F.col("customer_age_days") < 90,   "New (<90d)")
         .when(F.col("customer_age_days") < 365,  "Developing (90-365d)")
         .when(F.col("customer_age_days") < 1095, "Established (1-3yr)")
         .otherwise("Loyal (3yr+)")
    )
    .withColumn("churn_risk_label",
        F.when(F.col("churn_risk_score") >= 0.7, "HIGH")
         .when(F.col("churn_risk_score") >= 0.4, "MEDIUM")
         .otherwise("LOW")
    )
    .withColumn("customer_sk",      F.md5(F.col("customer_id").cast("string")))
    .withColumn("_gold_ingest_id",  F.lit(RUN_ID))
    .withColumn("_gold_ingest_ts",  F.current_timestamp())
    .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_is_current",      F.lit(True))
    .withColumn("_effective_from",  F.lit(BATCH_DATE).cast("date"))
)

# COMMAND ----------
# MAGIC %md ## 4. Write to Gold (Full Overwrite) & Optimize

# COMMAND ----------

(dim_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dim_{DIM_NAME}")
)

rows_out = spark.table(TGT_TABLE).count()
print(f"Written {rows_out:,} rows → {TGT_TABLE}")

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (customer_sk, customer_id, account_tier)")

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT account_tier, segment_code, churn_risk_label,
           COUNT(*) AS customer_count, AVG(credit_limit) AS avg_credit_limit
    FROM {TGT_TABLE}
    GROUP BY account_tier, segment_code, churn_risk_label
    ORDER BY account_tier, segment_code
"""))
