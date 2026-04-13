# Databricks notebook source
# MAGIC %md
# MAGIC # nb_gld_02_dimension_table_batch
# MAGIC **Layer:** Gold
# MAGIC **Pattern:** Conformed Dimension Table
# MAGIC **Purpose:** Publish a clean, business-labelled, BI-ready dimension table to the gold layer.
# MAGIC Pulls from silver (current snapshot or SCD2 history) and applies final business naming,
# MAGIC surrogate key generation, and BI-friendly derived attributes.
# MAGIC
# MAGIC **Table:** `gold.dim_<entity>`
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Building `dim_customer`, `dim_product`, `dim_date`, `dim_geography`
# MAGIC - Publishing conformed dimensions used across multiple fact tables
# MAGIC - Replacing Alteryx Join/Append workflows that produced lookup tables for BI

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",        "company_dev",  "Catalog")
dbutils.widgets.text("dim_name",       "customer",     "Dimension Name (suffix of dim_)")
dbutils.widgets.text("source_table",   "conformed__customer_360", "Silver Source Table")
dbutils.widgets.text("natural_key",    "customer_id",  "Natural Key Column")
dbutils.widgets.text("batch_date",     "",             "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env",        "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG      = dbutils.widgets.get("catalog")
DIM_NAME     = dbutils.widgets.get("dim_name")
SOURCE_TABLE = dbutils.widgets.get("source_table")
NAT_KEY      = dbutils.widgets.get("natural_key")

raw_bd       = dbutils.widgets.get("batch_date").strip()
BATCH_DATE   = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID       = str(uuid.uuid4())

SRC_TABLE    = f"`{CATALOG}`.`silver`.`{SOURCE_TABLE}`"
TGT_TABLE    = f"`{CATALOG}`.`gold`.`dim_{DIM_NAME}`"

print(f"Source : {SRC_TABLE}")
print(f"Target : {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 2. Read Silver (Current Snapshot)

# COMMAND ----------

silver_df = (spark.table(SRC_TABLE)
    .filter(F.col("_silver_batch_date") == F.lit(BATCH_DATE).cast("date"))
    .filter(F.col(NAT_KEY).isNotNull())
)
rows_in = silver_df.count()
print(f"Silver rows: {rows_in:,}")

# COMMAND ----------
# MAGIC %md ## 3. Select & Rename to Business-Friendly Names
# MAGIC
# MAGIC Edit this block to select the columns relevant to your dimension entity.
# MAGIC Follow naming convention: no abbreviations, no system prefixes in gold.

# COMMAND ----------

dim_df = silver_df.select(
    # Natural key (preserved for lineage/joins)
    F.col("customer_id"),

    # Business identifiers
    F.col("customer_name").alias("customer_full_name"),
    F.col("email").alias("email_address"),
    F.col("phone").alias("phone_number"),

    # Geography
    F.col("country").alias("country_name"),
    F.col("city").alias("city_name"),

    # Account attributes
    F.col("account_tier"),
    F.col("account_type"),
    F.col("account_status"),
    F.col("payment_terms"),
    F.col("credit_limit"),

    # Segmentation
    F.col("segment_code"),
    F.col("segment_label"),
    F.col("ltv_band"),
    F.col("churn_risk_score"),

    # Derived flags
    F.col("is_high_value"),
    F.col("customer_age_days"),

    # Dates
    F.col("created_at").alias("customer_since_ts"),
)

# COMMAND ----------
# MAGIC %md ## 4. Add Derived Business Attributes

# COMMAND ----------

dim_enriched = (dim_df

    # Tenure bucket
    .withColumn("customer_tenure_band",
        F.when(F.col("customer_age_days") < 90,   "New (<90d)")
         .when(F.col("customer_age_days") < 365,  "Developing (90-365d)")
         .when(F.col("customer_age_days") < 1095, "Established (1-3yr)")
         .otherwise("Loyal (3yr+)")
    )

    # Churn risk label
    .withColumn("churn_risk_label",
        F.when(F.col("churn_risk_score") >= 0.7, "HIGH")
         .when(F.col("churn_risk_score") >= 0.4, "MEDIUM")
         .otherwise("LOW")
    )

    # Surrogate key: deterministic hash for stable joins in downstream facts
    .withColumn("customer_sk",
        F.md5(F.col("customer_id").cast("string"))
    )

    # Gold metadata
    .withColumn("_gold_ingest_id",  F.lit(RUN_ID))
    .withColumn("_gold_ingest_ts",  F.current_timestamp())
    .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_is_current",      F.lit(True))
    .withColumn("_effective_from",  F.lit(BATCH_DATE).cast("date"))
)

print(f"Dimension schema ({len(dim_enriched.columns)} columns):")
dim_enriched.printSchema()

# COMMAND ----------
# MAGIC %md ## 5. Write to Gold (Full Overwrite — dimension is always current state)

# COMMAND ----------

(dim_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dim_{DIM_NAME}")
)

rows_out = spark.table(TGT_TABLE).count()
print(f"Written {rows_out:,} rows → {TGT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 6. Apply OPTIMIZE + ZORDER

# COMMAND ----------

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (customer_sk, customer_id, account_tier)")

# COMMAND ----------
# MAGIC %md ## 7. Output Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        account_tier,
        segment_code,
        churn_risk_label,
        COUNT(*)          AS customer_count,
        AVG(credit_limit) AS avg_credit_limit
    FROM {TGT_TABLE}
    GROUP BY account_tier, segment_code, churn_risk_label
    ORDER BY account_tier, segment_code
"""))

dbutils.jobs.taskValues.set(key="gold_dim_table", value=f"{CATALOG}.gold.dim_{DIM_NAME}")
dbutils.jobs.taskValues.set(key="rows_written",   value=rows_out)
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_out}")
