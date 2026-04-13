# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_04_multisource_join_batch
# MAGIC **Layer:** Silver | **Pattern:** Multi-Source Join / Conformed Entity
# MAGIC **Purpose:** Combine multiple bronze/silver sources into a single conformed silver entity.
# MAGIC Edit Section 2 to define your source tables and join conditions.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

CATALOG       = "company_dev"
OUTPUT_DOMAIN = "conformed"
OUTPUT_ENTITY = "customer_360"
BATCH_DATE    = str(__import__("datetime").date.today())

# ─────────────────────────────────────────────────────────────────────────────

import datetime, uuid
from pyspark.sql import functions as F

RUN_ID    = str(uuid.uuid4())
TGT_TABLE = f"`{CATALOG}`.`silver`.`{OUTPUT_DOMAIN}__{OUTPUT_ENTITY}`"

print(f"Target Table : {TGT_TABLE}")
print(f"Batch Date   : {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 1. Read Source Tables

# COMMAND ----------

# ── Source 1: Primary entity (spine) ─────────────────────────────────────────
customers_df = (spark.table(f"`{CATALOG}`.`bronze`.`crm__customers`")
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
    .filter(F.col("customer_id").isNotNull())
    .select("customer_id", "customer_name", "email", "phone",
            "country", "city", "created_at")
)
print(f"customers : {customers_df.count():,}")

# ── Source 2: ERP account data ────────────────────────────────────────────────
accounts_df = (spark.table(f"`{CATALOG}`.`bronze`.`erp__accounts`")
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
    .select("account_id", "account_type", "credit_limit", "payment_terms", "account_status")
    .withColumnRenamed("account_id", "customer_id")
)
print(f"accounts  : {accounts_df.count():,}")

# ── Source 3: Finance segments (already silver) ───────────────────────────────
segments_df = (spark.table(f"`{CATALOG}`.`silver`.`finance__segments`")
    .filter(F.col("_is_current") == True)
    .select("customer_id", "segment_code", "segment_label", "ltv_band", "churn_risk_score")
)
print(f"segments  : {segments_df.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Join Sources

# COMMAND ----------

joined_df = (customers_df
    .join(accounts_df, on="customer_id", how="left")
    .join(segments_df, on="customer_id", how="left")
)
rows_joined = joined_df.count()
print(f"Joined rows: {rows_joined:,}")

# COMMAND ----------
# MAGIC %md ## 3. Apply Business Rules

# COMMAND ----------

conformed_df = (joined_df
    .withColumn("account_tier",
        F.when(F.col("credit_limit") >= 100000, "ENTERPRISE")
         .when(F.col("credit_limit") >= 25000,  "MID_MARKET")
         .when(F.col("credit_limit") >= 5000,   "SMB")
         .otherwise("STARTER")
    )
    .withColumn("is_high_value",
        (F.col("ltv_band") == "A") & (F.col("account_tier") == "ENTERPRISE")
    )
    .withColumn("customer_age_days",
        F.datediff(F.current_date(), F.col("created_at").cast("date"))
    )
    .withColumn("segment_code",     F.coalesce(F.col("segment_code"),     F.lit("UNASSIGNED")))
    .withColumn("segment_label",    F.coalesce(F.col("segment_label"),    F.lit("Unassigned")))
    .withColumn("churn_risk_score", F.coalesce(F.col("churn_risk_score"), F.lit(0.0)))
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_lineage",    F.lit("crm__customers + erp__accounts + finance__segments"))
)

# COMMAND ----------
# MAGIC %md ## 4. Referential Integrity Warning

# COMMAND ----------

no_account = conformed_df.filter(F.col("account_type").isNull()).count()
no_segment = conformed_df.filter(F.col("segment_code") == "UNASSIGNED").count()
print(f"Customers without account data : {no_account:,}")
print(f"Customers without segment data : {no_segment:,}")

if rows_joined > 0 and (no_account / rows_joined) > 0.10:
    print("WARNING: >10% of customers lack ERP account data.")

# COMMAND ----------
# MAGIC %md ## 5. Write to Silver & Optimize

# COMMAND ----------

(conformed_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_silver_batch_date = '{BATCH_DATE}'")
    .option("mergeSchema", "true")
    .partitionBy("_silver_batch_date")
    .saveAsTable(f"{CATALOG}.silver.{OUTPUT_DOMAIN}__{OUTPUT_ENTITY}")
)

rows_written = conformed_df.count()
print(f"Written {rows_written:,} rows → {TGT_TABLE}")
spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (customer_id, account_tier)")
