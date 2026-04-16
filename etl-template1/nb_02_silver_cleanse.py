# Databricks notebook source
# MAGIC %md
# MAGIC # nb_02_silver_cleanse
# MAGIC **Layer:** Silver
# MAGIC **Purpose:** Read the four bronze tables, apply type casting, null handling, and
# MAGIC data quality checks. Join claims with members, providers, and plans to produce
# MAGIC a conformed `silver.claims_enriched` table ready for gold aggregations.
# MAGIC
# MAGIC **Bronze → Silver:**
# MAGIC ```
# MAGIC bronze.raw_plans      →  silver.plans
# MAGIC bronze.raw_members    →  silver.members
# MAGIC bronze.raw_providers  →  silver.providers
# MAGIC bronze.raw_claims     →  silver.claims
# MAGIC
# MAGIC silver.claims
# MAGIC   + silver.members    →  silver.claims_enriched  (conformed join)
# MAGIC   + silver.providers  (broadcast — small dimension)
# MAGIC   + silver.plans      (broadcast — small dimension)
# MAGIC ```

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
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
print(f"Run ID       : {RUN_ID}")
print(f"Batch Date   : {BATCH_DATE}")
print(f"Catalog      : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## Spark Performance Configuration
# MAGIC
# MAGIC | Setting | Value | Rationale |
# MAGIC |---|---|---|
# MAGIC | `adaptive.enabled` | true | AQE re-plans joins using runtime cardinality |
# MAGIC | `coalescePartitions.enabled` | true | Merges small partitions after shuffles |
# MAGIC | `skewJoin.enabled` | true | Handles skew in member/provider groupings |
# MAGIC | `shuffle.partitions` | 8 | Small dataset — default 200 creates near-empty tasks |
# MAGIC | `autoBroadcastJoinThreshold` | 20 MB | Plans (4 rows) and providers (8 rows) broadcast automatically |
# MAGIC | `optimizeWrite` | true | Delta auto-sizes Parquet files on write |
# MAGIC | `autoCompact` | true | Delta compacts incrementally after writes |

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",           "true")
spark.conf.set("spark.sql.shuffle.partitions",                  "8")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",          str(20 * 1024 * 1024))
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",  "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",    "true")

print("Spark performance settings applied.")

# COMMAND ----------
# MAGIC %md ## Helper — Drop Bronze Metadata Columns

# COMMAND ----------

def drop_meta(df):
    """Remove bronze internal metadata columns before silver write."""
    return df.drop(*[c for c in df.columns if c.startswith("_")])

# COMMAND ----------
# MAGIC %md ## 1. Cleanse Plans

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="health_insurance",
    entity_name="plans", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    plans_raw = spark.table(f"{CATALOG}.bronze.raw_plans")
    audit.set_rows_read(plans_raw.count())

    plans = (drop_meta(plans_raw)
        .withColumn("annual_deductible", F.col("annual_deductible").cast("double"))
        .withColumn("oop_max",           F.col("oop_max").cast("double"))
        .withColumn("premium_monthly",   F.col("premium_monthly").cast("double"))
        .withColumn("effective_date",    F.col("effective_date").cast("date"))
        .withColumn("termination_date",  F.col("termination_date").cast("date"))
        .withColumn("plan_name",         F.trim(F.col("plan_name")))
        .withColumn("plan_type",         F.upper(F.trim(F.col("plan_type"))))
        .withColumn("metal_tier",        F.initcap(F.trim(F.col("metal_tier"))))
        .withColumn("is_active",
            F.when(F.col("termination_date").isNull(), F.lit(True)).otherwise(F.lit(False)))
        .filter(F.col("plan_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (plans.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.plans"))

    rows_plans = plans.count()
    audit.set_rows_written(rows_plans)
    print(f"silver.plans: {rows_plans} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`plans` ZORDER BY (plan_id, plan_type)")

print("\n=== DQ: silver.plans ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.plans"),
    suite=[
        {"name": "not_null__plan_id",        "type": "not_null",        "column": "plan_id",        "threshold": 1.0},
        {"name": "unique__plan_id",          "type": "unique",          "column": "plan_id",        "threshold": 1.0},
        {"name": "range__annual_deductible", "type": "range",           "column": "annual_deductible","min": 0,       "threshold": 1.0},
        {"name": "range__oop_max",           "type": "range",           "column": "oop_max",        "min": 0,       "threshold": 1.0},
        {"name": "range__premium_monthly",   "type": "range",           "column": "premium_monthly","min": 0,       "threshold": 1.0},
        {"name": "valid_plan_type",          "type": "accepted_values", "column": "plan_type",
         "values": ["PPO","HMO","EPO","POS","HDHP"],                                                "threshold": 1.0},
    ],
    entity_name="plans", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 2. Cleanse Members

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="health_insurance",
    entity_name="members", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    members_raw = spark.table(f"{CATALOG}.bronze.raw_members")
    audit.set_rows_read(members_raw.count())

    members = (drop_meta(members_raw)
        .withColumn("dob",              F.col("dob").cast("date"))
        .withColumn("enrollment_date",  F.col("enrollment_date").cast("date"))
        .withColumn("termination_date", F.col("termination_date").cast("date"))
        .withColumn("is_active",        F.col("is_active").cast("boolean"))
        .withColumn("first_name",       F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",        F.initcap(F.trim(F.col("last_name"))))
        .withColumn("gender",           F.upper(F.trim(F.col("gender"))))
        .withColumn("state",            F.upper(F.trim(F.col("state"))))
        .withColumn("zip_code",         F.trim(F.col("zip_code")))
        # Derived: member age
        .withColumn("age",
            F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25).cast("int"))
        # Derived: age band for analytics
        .withColumn("age_band",
            F.when(F.col("age") < 18,  F.lit("0-17"))
             .when(F.col("age") < 35,  F.lit("18-34"))
             .when(F.col("age") < 50,  F.lit("35-49"))
             .when(F.col("age") < 65,  F.lit("50-64"))
             .otherwise(F.lit("65+"))
        )
        .filter(F.col("member_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (members.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.members"))

    rows_members = members.count()
    audit.set_rows_written(rows_members)
    print(f"silver.members: {rows_members} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`members` ZORDER BY (member_id, plan_id, state)")

print("\n=== DQ: silver.members ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.members"),
    suite=[
        {"name": "not_null__member_id",     "type": "not_null", "column": "member_id",     "threshold": 1.0},
        {"name": "unique__member_id",       "type": "unique",   "column": "member_id",     "threshold": 1.0},
        {"name": "not_null__plan_id",       "type": "not_null", "column": "plan_id",       "threshold": 1.0},
        {"name": "not_null__enrollment_date","type": "not_null","column": "enrollment_date","threshold": 1.0},
        {"name": "range__age",              "type": "range",    "column": "age", "min": 0, "max": 120, "threshold": 1.0},
    ],
    entity_name="members", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 3. Cleanse Providers

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="health_insurance",
    entity_name="providers", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    providers_raw = spark.table(f"{CATALOG}.bronze.raw_providers")
    audit.set_rows_read(providers_raw.count())

    providers = (drop_meta(providers_raw)
        .withColumn("is_in_network",  F.col("is_in_network").cast("boolean"))
        .withColumn("provider_name",  F.trim(F.col("provider_name")))
        .withColumn("provider_type",  F.trim(F.col("provider_type")))
        .withColumn("specialty",      F.trim(F.col("specialty")))
        .withColumn("state",          F.upper(F.trim(F.col("state"))))
        .withColumn("network_tier",   F.trim(F.col("network_tier")))
        .withColumn("npi_number",     F.trim(F.col("npi_number")))
        .filter(F.col("provider_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (providers.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.silver.providers"))

    rows_providers = providers.count()
    audit.set_rows_written(rows_providers)
    print(f"silver.providers: {rows_providers} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`providers` ZORDER BY (provider_id, provider_type)")

print("\n=== DQ: silver.providers ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.providers"),
    suite=[
        {"name": "not_null__provider_id",   "type": "not_null", "column": "provider_id",   "threshold": 1.0},
        {"name": "unique__provider_id",     "type": "unique",   "column": "provider_id",   "threshold": 1.0},
        {"name": "not_null__provider_name", "type": "not_null", "column": "provider_name", "threshold": 1.0},
        {"name": "not_null__npi_number",    "type": "not_null", "column": "npi_number",    "threshold": 1.0},
        {"name": "npi_10_digits",           "type": "regex",    "column": "npi_number",
         "pattern": r"^\d{10}$",                                                           "threshold": 1.0},
    ],
    entity_name="providers", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 4. Cleanse Claims

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="health_insurance",
    entity_name="claims", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    claims_raw = spark.table(f"{CATALOG}.bronze.raw_claims")
    audit.set_rows_read(claims_raw.count())

    claims = (drop_meta(claims_raw)
        .withColumn("claim_date",         F.col("claim_date").cast("date"))
        .withColumn("service_date",       F.col("service_date").cast("date"))
        .withColumn("billed_amount",      F.col("billed_amount").cast("double"))
        .withColumn("allowed_amount",     F.col("allowed_amount").cast("double"))
        .withColumn("paid_amount",        F.col("paid_amount").cast("double"))
        .withColumn("member_copay",       F.col("member_copay").cast("double"))
        .withColumn("member_deductible",  F.col("member_deductible").cast("double"))
        .withColumn("claim_type",         F.upper(F.trim(F.col("claim_type"))))
        .withColumn("claim_status",       F.upper(F.trim(F.col("claim_status"))))
        .withColumn("diagnosis_code",     F.trim(F.col("diagnosis_code")))
        .withColumn("procedure_code",     F.trim(F.col("procedure_code")))
        # Derived: member out-of-pocket = copay + deductible
        .withColumn("member_oop",
            F.round(F.col("member_copay") + F.col("member_deductible"), 2))
        # Derived: plan discount = billed - allowed
        .withColumn("discount_amount",
            F.round(F.col("billed_amount") - F.col("allowed_amount"), 2))
        # Derived: claim year/month for partitioning
        .withColumn("claim_year",  F.year("claim_date"))
        .withColumn("claim_month", F.month("claim_date"))
        .filter(F.col("claim_id").isNotNull())
        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    (claims.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("claim_year", "claim_month")
        .saveAsTable(f"{CATALOG}.silver.claims"))

    rows_claims = claims.count()
    audit.set_rows_written(rows_claims)
    print(f"silver.claims: {rows_claims} rows")

spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`claims`"
          f" ZORDER BY (member_id, provider_id, claim_type)")

print("\n=== DQ: silver.claims ===")
run_dq_suite(
    df=spark.table(f"{CATALOG}.silver.claims"),
    suite=[
        {"name": "not_null__claim_id",    "type": "not_null",        "column": "claim_id",     "threshold": 1.0},
        {"name": "unique__claim_id",      "type": "unique",          "column": "claim_id",     "threshold": 1.0},
        {"name": "not_null__member_id",   "type": "not_null",        "column": "member_id",    "threshold": 1.0},
        {"name": "not_null__provider_id", "type": "not_null",        "column": "provider_id",  "threshold": 1.0},
        {"name": "range__billed_amount",  "type": "range",           "column": "billed_amount","min": 0, "threshold": 1.0},
        {"name": "range__paid_amount",    "type": "range",           "column": "paid_amount",  "min": 0, "threshold": 1.0},
        {"name": "valid_claim_status",    "type": "accepted_values", "column": "claim_status",
         "values": ["PAID","PENDING","DENIED","VOID"],                                          "threshold": 1.0},
    ],
    entity_name="claims", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

# COMMAND ----------
# MAGIC %md ## 5. Build Conformed Join — `claims_enriched`
# MAGIC
# MAGIC Join all four silver tables into a single wide table that gold can aggregate
# MAGIC without further joins.
# MAGIC
# MAGIC **Performance:** `plans` (4 rows) and `providers` (8 rows) are broadcast to every
# MAGIC executor — zero shuffle cost for those joins. `claims_enriched` is cached before
# MAGIC write so the row count does not recompute the full join plan.

# COMMAND ----------

with PipelineAudit(
    catalog=CATALOG, pipeline_name="nb_02_silver_cleanse",
    layer="silver", source_system="health_insurance",
    entity_name="claims_enriched", batch_date=BATCH_DATE,
    load_type="full", run_id=RUN_ID,
) as audit:

    sv_claims    = spark.table(f"{CATALOG}.silver.claims")
    sv_members   = spark.table(f"{CATALOG}.silver.members").drop("_silver_ts","_silver_batch_date")
    sv_providers = spark.table(f"{CATALOG}.silver.providers").drop("_silver_ts","_silver_batch_date")
    sv_plans     = spark.table(f"{CATALOG}.silver.plans").drop("_silver_ts","_silver_batch_date")

    audit.set_rows_read(sv_claims.count())

    claims_enriched = (sv_claims

        # Join member dimension (shuffle join — moderate size)
        .join(sv_members.select(
            "member_id","plan_id","first_name","last_name","dob","gender",
            "state","age","age_band","enrollment_date","is_active"
        ), on="member_id", how="left")

        # Broadcast provider — 8 rows, no shuffle
        .join(F.broadcast(sv_providers.select(
            "provider_id","provider_name","provider_type","specialty",
            "network_tier","is_in_network"
        )), on="provider_id", how="left")

        # Broadcast plan — 4 rows, no shuffle
        .join(F.broadcast(sv_plans.select(
            "plan_id","plan_name","plan_type","metal_tier",
            "annual_deductible","oop_max","premium_monthly"
        )), on="plan_id", how="left")

        .withColumn("_silver_ts",         F.current_timestamp())
        .withColumn("_silver_batch_date", F.lit(BATCH_DATE).cast("date"))
    )

    # Cache before write — avoids recomputing the multi-way join for count()
    claims_enriched.cache()

    (claims_enriched.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("claim_year", "claim_month")
        .saveAsTable(f"{CATALOG}.silver.claims_enriched"))

    rows_enriched = claims_enriched.count()   # served from cache
    claims_enriched.unpersist()
    audit.set_rows_written(rows_enriched)
    print(f"silver.claims_enriched: {rows_enriched} rows  (grain: one row per claim)")

spark.sql(f"OPTIMIZE `{CATALOG}`.`silver`.`claims_enriched`"
          f" ZORDER BY (member_id, provider_id, claim_type, claim_date)")

# COMMAND ----------
# MAGIC %md ## 6. DQ Summary — `claims_enriched`

# COMMAND ----------

enriched_df = spark.table(f"{CATALOG}.silver.claims_enriched")

print("\n=== DQ: silver.claims_enriched ===")
run_dq_suite(
    df=enriched_df,
    suite=[
        {"name": "not_null__claim_id",        "type": "not_null", "column": "claim_id",       "threshold": 1.0},
        {"name": "not_null__member_id",       "type": "not_null", "column": "member_id",      "threshold": 1.0},
        {"name": "join_member_name_match",    "type": "not_null", "column": "last_name",      "threshold": 0.99},
        {"name": "join_provider_name_match",  "type": "not_null", "column": "provider_name",  "threshold": 0.99},
        {"name": "join_plan_name_match",      "type": "not_null", "column": "plan_name",      "threshold": 0.99},
        {"name": "row_count_min_1",           "type": "row_count_threshold", "min_rows": 1,   "threshold": 1.0},
    ],
    entity_name="claims_enriched", layer="silver",
    run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE,
)

display(enriched_df.select(
    "claim_id","member_id","last_name","provider_name","plan_name",
    "claim_type","billed_amount","paid_amount","claim_status","claim_date"
).orderBy("claim_date").limit(10))

# COMMAND ----------

print(f"\nSilver cleanse complete.")
print(f"  silver.plans            : {rows_plans} rows")
print(f"  silver.members          : {rows_members} rows")
print(f"  silver.providers        : {rows_providers} rows")
print(f"  silver.claims           : {rows_claims} rows")
print(f"  silver.claims_enriched  : {rows_enriched} rows")
print(f"  Physical storage : {ADLS_BASE}/catalog/{CATALOG}/silver/")
print("  Next: Run nb_03_gold_mart.py")
