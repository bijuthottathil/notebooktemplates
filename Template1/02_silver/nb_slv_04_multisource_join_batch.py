# Databricks notebook source
# MAGIC %md
# MAGIC # nb_slv_04_multisource_join_batch
# MAGIC **Layer:** Silver
# MAGIC **Pattern:** Multi-Source Join / Conformed Entity
# MAGIC **Purpose:** Combine multiple bronze/silver sources into a single conformed silver entity.
# MAGIC Typical use case is joining a primary entity from one source with enrichment data
# MAGIC from other systems — the Alteryx "Join tool" pattern at scale.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC bronze.crm__customers   ──┐
# MAGIC bronze.erp__accounts    ──┤  JOIN  →  silver.conformed__customer_360
# MAGIC silver.finance__segments──┘
# MAGIC ```
# MAGIC
# MAGIC **When to use this template:**
# MAGIC - Building 360-degree views (customer_360, product_360)
# MAGIC - Enriching transactional data with reference/dimension data
# MAGIC - Replacing Alteryx workflows with multiple input streams and a join tool

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",           "company_dev",     "Catalog")
dbutils.widgets.text("output_domain",     "conformed",       "Output Domain (Silver Schema)")
dbutils.widgets.text("output_entity",     "customer_360",    "Output Entity Name")
dbutils.widgets.text("batch_date",        "",                "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("env",           "dev", ["dev", "staging", "prod"], "Environment")

import datetime, uuid
from pyspark.sql import functions as F

CATALOG        = dbutils.widgets.get("catalog")
OUTPUT_DOMAIN  = dbutils.widgets.get("output_domain")
OUTPUT_ENTITY  = dbutils.widgets.get("output_entity")

raw_bd         = dbutils.widgets.get("batch_date").strip()
BATCH_DATE     = raw_bd if raw_bd else str(datetime.date.today())
RUN_ID         = str(uuid.uuid4())

TGT_TABLE      = f"`{CATALOG}`.`silver`.`{OUTPUT_DOMAIN}__{OUTPUT_ENTITY}`"

print(f"Target Table : {TGT_TABLE}")
print(f"Batch Date   : {BATCH_DATE}")

# COMMAND ----------
# MAGIC %md ## 2. Source Configuration
# MAGIC
# MAGIC Edit this section to define your source tables and join logic.
# MAGIC Follows the Airbnb pattern of explicit source mapping with documented lineage.

# COMMAND ----------

# ── SOURCE 1: Primary entity ─────────────────────────────────────────────────
customers_df = (spark.table(f"`{CATALOG}`.`bronze`.`crm__customers`")
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "country",
        "city",
        "created_at",
        "_ingest_date"
    )
)
print(f"Source 1 — customers: {customers_df.count():,} rows")

# ── SOURCE 2: Account data from ERP ──────────────────────────────────────────
accounts_df = (spark.table(f"`{CATALOG}`.`bronze`.`erp__accounts`")
    .filter(F.col("_ingest_date") == F.lit(BATCH_DATE).cast("date"))
    .filter(F.col("account_id").isNotNull())
    .select(
        "account_id",       # joins to customer_id
        "account_type",
        "credit_limit",
        "payment_terms",
        "account_status"
    )
    .withColumnRenamed("account_id", "customer_id")   # normalise join key
)
print(f"Source 2 — accounts : {accounts_df.count():,} rows")

# ── SOURCE 3: Segment from Finance (already silver) ──────────────────────────
segments_df = (spark.table(f"`{CATALOG}`.`silver`.`finance__segments`")
    .filter(F.col("_is_current") == True)
    .select(
        "customer_id",
        "segment_code",
        "segment_label",
        "ltv_band",
        "churn_risk_score"
    )
)
print(f"Source 3 — segments : {segments_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 3. Join Sources

# COMMAND ----------

# Left-join strategy: customer is always the spine; enrichment data is optional
joined_df = (customers_df
    .join(accounts_df, on="customer_id", how="left")
    .join(segments_df, on="customer_id", how="left")
)

rows_joined = joined_df.count()
print(f"Joined rows: {rows_joined:,}")

# COMMAND ----------
# MAGIC %md ## 4. Apply Business Rules & Derive Columns

# COMMAND ----------

# Add derived columns that represent business logic — document each rule
conformed_df = (joined_df

    # Rule: Classify account tier based on credit limit
    .withColumn("account_tier",
        F.when(F.col("credit_limit") >= 100000, "ENTERPRISE")
         .when(F.col("credit_limit") >= 25000,  "MID_MARKET")
         .when(F.col("credit_limit") >= 5000,   "SMB")
         .otherwise("STARTER")
    )

    # Rule: Mark high-value customers (LTV band A + Enterprise tier)
    .withColumn("is_high_value",
        (F.col("ltv_band") == "A") & (F.col("account_tier") == "ENTERPRISE")
    )

    # Rule: Derive customer age in days (from CRM created_at)
    .withColumn("customer_age_days",
        F.datediff(F.current_date(), F.col("created_at").cast("date"))
    )

    # Standardise nulls in enrichment columns (not all customers have segment)
    .withColumn("segment_code",    F.coalesce(F.col("segment_code"),    F.lit("UNASSIGNED")))
    .withColumn("segment_label",   F.coalesce(F.col("segment_label"),   F.lit("Unassigned")))
    .withColumn("churn_risk_score",F.coalesce(F.col("churn_risk_score"),F.lit(0.0)))

    # Silver metadata
    .withColumn("_silver_ingest_id",  F.lit(RUN_ID))
    .withColumn("_silver_ingest_ts",  F.current_timestamp())
    .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    .withColumn("_source_lineage",    F.lit("crm__customers + erp__accounts + finance__segments"))
)

print(f"Conformed schema ({len(conformed_df.columns)} columns):")
conformed_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 5. Referential Integrity Checks

# COMMAND ----------

# Flag records where enrichment join produced nulls (indicates upstream data issue)
customers_without_account = conformed_df.filter(F.col("account_type").isNull()).count()
customers_without_segment = conformed_df.filter(F.col("segment_code") == "UNASSIGNED").count()

print(f"Customers without account data : {customers_without_account:,}")
print(f"Customers without segment data : {customers_without_segment:,}")

# If > 10% of customers lack account data, raise a warning (adjust threshold as needed)
if rows_joined > 0 and (customers_without_account / rows_joined) > 0.10:
    print(f"WARNING: >10% of customers lack ERP account data. Investigate erp__accounts coverage.")

# COMMAND ----------
# MAGIC %md ## 6. Write to Silver (Replace this batch)

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

# COMMAND ----------
# MAGIC %md ## 7. Optimize

# COMMAND ----------

spark.sql(f"OPTIMIZE {TGT_TABLE} ZORDER BY (customer_id, account_tier)")

# COMMAND ----------
# MAGIC %md ## 8. Output

# COMMAND ----------

dbutils.jobs.taskValues.set(key="silver_table",   value=f"{CATALOG}.silver.{OUTPUT_DOMAIN}__{OUTPUT_ENTITY}")
dbutils.jobs.taskValues.set(key="rows_written",   value=rows_written)
dbutils.jobs.taskValues.set(key="run_id",         value=RUN_ID)

dbutils.notebook.exit(f"SUCCESS|{rows_written}")
