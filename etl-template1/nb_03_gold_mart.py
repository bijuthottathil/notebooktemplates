# Databricks notebook source
# MAGIC %md
# MAGIC # nb_03_gold_mart
# MAGIC **Layer:** Gold
# MAGIC **Purpose:** Build four business-ready gold tables from `silver.claims_enriched`.
# MAGIC No joins required — silver already conformed everything.
# MAGIC
# MAGIC **Gold Tables Produced:**
# MAGIC | Table | Grain | Use Case |
# MAGIC |---|---|---|
# MAGIC | `gold.fct_claims` | One row per claim | Detailed fact for ad-hoc analysis |
# MAGIC | `gold.rpt_member_utilization` | One row per member | Utilization and cost per member |
# MAGIC | `gold.rpt_provider_performance` | One row per provider | Cost, volume, and network metrics |
# MAGIC | `gold.rpt_spend_by_plan` | One row per plan × claim type × month | Spend analytics for actuarial/finance |

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
STORAGE_ACCOUNT = "dlshealthdev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template1"

# ── Pipeline ──────────────────────────────────────────────────────────────────
CATALOG    = "health_insurance_dev"
BATCH_DATE = "2024-07-30"

# ─────────────────────────────────────────────────────────────────────────────

import uuid
from pyspark.sql import functions as F, Window

RUN_ID = str(uuid.uuid4())
SOURCE = f"{CATALOG}.silver.claims_enriched"
print(f"Catalog      : {CATALOG}")
print(f"Source Table : {SOURCE}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration
# MAGIC
# MAGIC | Setting | Value | Rationale |
# MAGIC |---|---|---|
# MAGIC | `adaptive.enabled` | true | AQE adapts aggregation and window plans at runtime |
# MAGIC | `coalescePartitions.enabled` | true | Merges small partitions after groupBy shuffles |
# MAGIC | `skewJoin.enabled` | true | Handles potential skew in member/provider groupings |
# MAGIC | `shuffle.partitions` | 8 | Small dataset — prevents 200 near-empty tasks |
# MAGIC | `windowExec.buffer.spill.threshold` | 4096 | Controls in-memory buffer per window before disk spill |
# MAGIC | `optimizeWrite` | true | Delta auto-packs Parquet files to right size |
# MAGIC | `autoCompact` | true | Delta compacts after writes — reduces future scan cost |

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.shuffle.partitions",                        "8")
spark.conf.set("spark.sql.windowExec.buffer.spill.threshold",         "4096")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",        "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",          "true")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## 1. Read & Cache Silver Source
# MAGIC
# MAGIC All four gold tables are derived from the same source. Caching it once prevents
# MAGIC four separate ADLS scans. `source_rows = silver.count()` materialises the cache
# MAGIC before any transforms begin.

# COMMAND ----------

silver = spark.table(SOURCE)
silver.cache()
source_rows = silver.count()   # materialise cache
print(f"Source rows: {source_rows:,}  (cached)")
silver.printSchema()

# COMMAND ----------
# MAGIC %md ## 2. Gold Fact — `fct_claims`
# MAGIC
# MAGIC Detailed fact table: one row per claim, fully enriched with member, provider, and
# MAGIC plan attributes. Partitioned by `claim_year` + `claim_month` so monthly dashboard
# MAGIC queries scan only the relevant partitions.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="health_insurance",
    entity_name="fct_claims", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    fct_claims = (silver
        .select(
            "claim_id","member_id","provider_id","plan_id",
            "last_name","first_name","gender","age","age_band","state",
            "provider_name","provider_type","specialty","network_tier","is_in_network",
            "plan_name","plan_type","metal_tier","premium_monthly",
            "claim_date","service_date","claim_year","claim_month",
            "claim_type","claim_status",
            "diagnosis_code","procedure_code",
            "billed_amount","allowed_amount","paid_amount",
            "member_copay","member_deductible","member_oop","discount_amount",
        )
        # Derived: insurer net paid (paid minus member share)
        .withColumn("insurer_paid_amount",
            F.round(F.col("paid_amount") - F.col("member_oop"), 2))
        # Derived: claim quarter
        .withColumn("claim_quarter", F.quarter("claim_date"))
        # Surrogate key (deterministic for idempotent reruns)
        .withColumn("fct_claim_sk", F.md5(F.col("claim_id")))
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (fct_claims.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("claim_year", "claim_month")
        .saveAsTable(f"{CATALOG}.gold.fct_claims"))

    rows_fct = fct_claims.count()
    audit.set_rows_written(rows_fct)
    print(f"gold.fct_claims: {rows_fct} rows")

# ZORDER by the most common BI filter columns within each date partition
spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`fct_claims`"
          f" ZORDER BY (member_id, claim_type, claim_status)")

# COMMAND ----------
# MAGIC %md ## 3. Report — `rpt_member_utilization`
# MAGIC
# MAGIC One row per member. Summarises total spend, claim frequency, and distinct provider
# MAGIC contacts. Useful for identifying high-utilisation members and care gap analysis.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="health_insurance",
    entity_name="rpt_member_utilization", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    rpt_member = (fct_claims
        .filter(F.col("claim_status").isin("PAID","PENDING"))
        .groupBy("member_id","first_name","last_name","gender","age","age_band",
                 "state","plan_id","plan_name","plan_type","metal_tier")
        .agg(
            F.count("claim_id")                          .alias("total_claims"),
            F.countDistinct("provider_id")               .alias("distinct_providers"),
            F.sum("billed_amount")                       .alias("total_billed"),
            F.sum("allowed_amount")                      .alias("total_allowed"),
            F.sum("paid_amount")                         .alias("total_paid"),
            F.sum("member_oop")                          .alias("total_member_oop"),
            F.sum(F.when(F.col("claim_type") == "MEDICAL",   F.col("paid_amount"))
                  .otherwise(0))                         .alias("medical_paid"),
            F.sum(F.when(F.col("claim_type") == "PHARMACY",  F.col("paid_amount"))
                  .otherwise(0))                         .alias("pharmacy_paid"),
            F.sum(F.when(F.col("claim_type") == "DENTAL",    F.col("paid_amount"))
                  .otherwise(0))                         .alias("dental_paid"),
            F.min("claim_date")                          .alias("first_claim_date"),
            F.max("claim_date")                          .alias("last_claim_date"),
        )
        .withColumn("avg_paid_per_claim",
            F.round(F.col("total_paid") / F.col("total_claims"), 2))
        .withColumn("billed_to_allowed_ratio",
            F.when(F.col("total_billed") > 0,
                F.round(F.col("total_allowed") / F.col("total_billed"), 3)
            ).otherwise(F.lit(None))
        )
        # Utilisation rank (most expensive member = rank 1)
        .withColumn("utilization_rank",
            F.rank().over(Window.orderBy(F.col("total_paid").desc()))
        )
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
        .orderBy("utilization_rank")
    )

    (rpt_member.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.rpt_member_utilization"))

    rows_member = rpt_member.count()
    audit.set_rows_written(rows_member)
    print(f"gold.rpt_member_utilization: {rows_member} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`rpt_member_utilization`"
          f" ZORDER BY (utilization_rank, plan_type, state)")

# COMMAND ----------
# MAGIC %md ## 4. Report — `rpt_provider_performance`
# MAGIC
# MAGIC One row per provider. Tracks volume, average claim cost, network tier, and
# MAGIC discount effectiveness. Supports network adequacy and contract management.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="health_insurance",
    entity_name="rpt_provider_performance", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    rpt_provider = (fct_claims
        .filter(F.col("claim_status") == "PAID")
        .groupBy("provider_id","provider_name","provider_type","specialty",
                 "network_tier","is_in_network")
        .agg(
            F.count("claim_id")                       .alias("total_claims"),
            F.countDistinct("member_id")              .alias("unique_members"),
            F.sum("billed_amount")                    .alias("total_billed"),
            F.sum("allowed_amount")                   .alias("total_allowed"),
            F.sum("paid_amount")                      .alias("total_paid"),
            F.avg("billed_amount")                    .alias("avg_billed_per_claim"),
            F.avg("paid_amount")                      .alias("avg_paid_per_claim"),
            F.sum("discount_amount")                  .alias("total_discount"),
        )
        .withColumn("discount_rate_pct",
            F.when(F.col("total_billed") > 0,
                F.round(F.col("total_discount") / F.col("total_billed") * 100, 1)
            ).otherwise(F.lit(None))
        )
        .withColumn("avg_billed_per_claim",  F.round(F.col("avg_billed_per_claim"), 2))
        .withColumn("avg_paid_per_claim",    F.round(F.col("avg_paid_per_claim"), 2))
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
        .orderBy(F.col("total_paid").desc())
    )

    (rpt_provider.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.gold.rpt_provider_performance"))

    rows_provider = rpt_provider.count()
    audit.set_rows_written(rows_provider)
    print(f"gold.rpt_provider_performance: {rows_provider} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`rpt_provider_performance`"
          f" ZORDER BY (provider_type, network_tier)")

# COMMAND ----------
# MAGIC %md ## 5. Report — `rpt_spend_by_plan`
# MAGIC
# MAGIC Monthly spend by plan × claim type. Includes month-over-month change per plan.
# MAGIC Supports actuarial analysis, premium adequacy review, and budget forecasting.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_03_gold_mart",
    layer="gold", source_system="health_insurance",
    entity_name="rpt_spend_by_plan", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    audit.set_rows_read(source_rows)

    monthly_plan = (fct_claims
        .filter(F.col("claim_status") == "PAID")
        .groupBy("claim_year","claim_month","plan_id","plan_name","plan_type","metal_tier","claim_type")
        .agg(
            F.count("claim_id")          .alias("total_claims"),
            F.countDistinct("member_id") .alias("unique_members"),
            F.sum("billed_amount")       .alias("total_billed"),
            F.sum("allowed_amount")      .alias("total_allowed"),
            F.sum("paid_amount")         .alias("total_paid"),
            F.sum("member_oop")          .alias("total_member_oop"),
            F.avg("paid_amount")         .alias("avg_paid_per_claim"),
        )
        .withColumn("month_label",
            F.concat(F.col("claim_year").cast("string"), F.lit("-"),
                     F.lpad(F.col("claim_month").cast("string"), 2, "0"))
        )
        .withColumn("avg_paid_per_claim", F.round(F.col("avg_paid_per_claim"), 2))
    )

    # Month-over-month change partitioned by plan + claim_type
    w_mom = Window.partitionBy("plan_id","claim_type").orderBy("claim_year","claim_month")

    rpt_plan = (monthly_plan
        .withColumn("prev_month_paid", F.lag("total_paid", 1).over(w_mom))
        .withColumn("mom_paid_change_pct",
            F.when(
                F.col("prev_month_paid").isNotNull() & (F.col("prev_month_paid") > 0),
                F.round(
                    (F.col("total_paid") - F.col("prev_month_paid"))
                    / F.col("prev_month_paid") * 100, 1
                )
            ).otherwise(F.lit(None))
        )
        .withColumn("_gold_ts",         F.current_timestamp())
        .withColumn("_gold_batch_date", F.lit(BATCH_DATE).cast("date"))
        .orderBy("claim_year","claim_month","plan_type","claim_type")
    )

    (rpt_plan.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("claim_year","claim_month")
        .saveAsTable(f"{CATALOG}.gold.rpt_spend_by_plan"))

    rows_plan = rpt_plan.count()
    audit.set_rows_written(rows_plan)
    print(f"gold.rpt_spend_by_plan: {rows_plan} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`gold`.`rpt_spend_by_plan`"
          f" ZORDER BY (plan_type, claim_type)")

# Release cached silver now that all four gold tables are built
silver.unpersist()
print("Silver cache released.")

# COMMAND ----------
# MAGIC %md ## 6. DQ Checks — Gold Fact

# COMMAND ----------

print("\n=== DQ: gold.fct_claims ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.gold.fct_claims"),
    suite=[
        {"name": "not_null__fct_claim_sk",   "type": "not_null",            "column": "fct_claim_sk",    "threshold": 1.0},
        {"name": "unique__fct_claim_sk",     "type": "unique",              "column": "fct_claim_sk",    "threshold": 1.0},
        {"name": "not_null__member_id",      "type": "not_null",            "column": "member_id",       "threshold": 1.0},
        {"name": "not_null__provider_id",    "type": "not_null",            "column": "provider_id",     "threshold": 1.0},
        {"name": "range__billed_amount",     "type": "range",               "column": "billed_amount",   "min": 0, "threshold": 1.0},
        {"name": "range__paid_amount",       "type": "range",               "column": "paid_amount",     "min": 0, "threshold": 1.0},
        {"name": "valid_claim_status",       "type": "accepted_values",     "column": "claim_status",
         "values": ["PAID","PENDING","DENIED","VOID"],                                                    "threshold": 1.0},
        {"name": "row_count_min_1",          "type": "row_count_threshold", "min_rows": 1,               "threshold": 1.0},
    ],
    entity_name="fct_claims", layer="gold",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 7. Results Preview

# COMMAND ----------

print("=== Member Utilization ===")
display(spark.sql(f"""
    SELECT last_name, first_name, plan_name, age_band, state,
           total_claims, distinct_providers,
           total_billed, total_paid, total_member_oop, utilization_rank
    FROM {CATALOG}.gold.rpt_member_utilization
    ORDER BY utilization_rank
"""))

print("=== Provider Performance ===")
display(spark.sql(f"""
    SELECT provider_name, provider_type, specialty, network_tier,
           total_claims, unique_members, total_billed, total_paid,
           avg_paid_per_claim, discount_rate_pct
    FROM {CATALOG}.gold.rpt_provider_performance
    ORDER BY total_paid DESC
"""))

print("=== Spend by Plan (monthly) ===")
display(spark.sql(f"""
    SELECT month_label, plan_name, plan_type, claim_type,
           total_claims, unique_members, total_paid, mom_paid_change_pct
    FROM {CATALOG}.gold.rpt_spend_by_plan
    ORDER BY month_label, total_paid DESC
"""))

# COMMAND ----------

print("\n" + "="*55)
print("GOLD MART COMPLETE")
print("="*55)
print(f"  fct_claims                : {rows_fct} rows")
print(f"  rpt_member_utilization    : {rows_member} rows")
print(f"  rpt_provider_performance  : {rows_provider} rows")
print(f"  rpt_spend_by_plan         : {rows_plan} rows")
print(f"  Physical storage          : {ADLS_BASE}/catalog/{CATALOG}/gold/")
print("="*55)
