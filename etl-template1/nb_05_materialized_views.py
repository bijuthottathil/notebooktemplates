# Databricks notebook source
# MAGIC %md
# MAGIC # nb_05_materialized_views
# MAGIC **Layer:** Gold — Materialized Views
# MAGIC **Purpose:** Create three Unity Catalog materialized views on top of the gold tables.
# MAGIC
# MAGIC | View | Source | Refresh Mode | Why |
# MAGIC |---|---|---|---|
# MAGIC | `mv_monthly_claims_summary` | `fct_claims` | **Incremental** | Simple GROUP BY + SUM/COUNT — no window functions |
# MAGIC | `mv_member_plan_spend` | `fct_claims` | **Incremental** | Simple GROUP BY on member × plan — equi-join friendly |
# MAGIC | `mv_provider_rank_by_spend` | `rpt_provider_performance` | **Full only** | Uses `RANK()` window function — cannot be maintained incrementally |
# MAGIC
# MAGIC **What makes a materialized view incrementally refreshable?**
# MAGIC
# MAGIC Databricks tracks row-level changes on source Delta tables via **Change Data Feed (CDF)**.
# MAGIC When a refresh runs, only the changed rows are re-aggregated and merged into the MV —
# MAGIC instead of recomputing the entire result set from scratch.
# MAGIC
# MAGIC Incremental refresh requires **all** of the following:
# MAGIC - Source Delta tables have `delta.enableChangeDataFeed = true`
# MAGIC - The query uses only incrementally-maintainable operations:
# MAGIC   - `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` aggregations
# MAGIC   - `GROUP BY` with simple expressions
# MAGIC   - `INNER JOIN` / `LEFT JOIN` on equality conditions
# MAGIC   - `WHERE` filters on non-generated columns
# MAGIC - **No** window functions (`RANK`, `LAG`, `ROW_NUMBER`, etc.)
# MAGIC - **No** `COUNT(DISTINCT ...)` in most cases
# MAGIC - **No** non-deterministic functions (`RAND`, `UUID`, `CURRENT_TIMESTAMP` as a measure)
# MAGIC - **No** `LIMIT` / `OFFSET`

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

CATALOG    = "health_insurance_dev"
BATCH_DATE = "2024-07-30"

print(f"Catalog : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Enable Change Data Feed on Source Gold Tables
# MAGIC
# MAGIC CDF is the prerequisite for incremental MV refresh. It records every insert,
# MAGIC update, and delete at the row level so Databricks can apply only the delta
# MAGIC during a refresh instead of recomputing everything.
# MAGIC
# MAGIC **Run this once.** CDF adds a small write overhead (~2–5%) but enables
# MAGIC incremental refresh, which can be orders of magnitude faster for large tables.

# COMMAND ----------

for table in ["fct_claims", "rpt_provider_performance", "rpt_member_utilization", "rpt_spend_by_plan"]:
    spark.sql(f"""
        ALTER TABLE `{CATALOG}`.`gold`.`{table}`
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    print(f"  [OK] CDF enabled: {CATALOG}.gold.{table}")

# COMMAND ----------
# MAGIC %md ## Materialized View 1 — `mv_monthly_claims_summary`
# MAGIC **Refresh mode: INCREMENTAL**
# MAGIC
# MAGIC Monthly aggregation of paid claims by claim type and plan type.
# MAGIC Contains only `SUM`, `COUNT`, and `AVG` — no window functions or DISTINCT counts.
# MAGIC When new claims land in `fct_claims`, only the affected year/month partitions
# MAGIC are re-aggregated during an incremental refresh.
# MAGIC
# MAGIC **Typical use:** Finance dashboards, monthly spend trend reports.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{CATALOG}`.`gold`.`mv_monthly_claims_summary`
    COMMENT 'Monthly paid claims summary by claim type and plan type. Supports incremental refresh.'
    TBLPROPERTIES (
        'pipelines.channel'          = 'PREVIEW',
        'delta.enableChangeDataFeed' = 'true'
    )
    AS
    SELECT
        claim_year,
        claim_month,
        CONCAT(CAST(claim_year AS STRING), '-',
               LPAD(CAST(claim_month AS STRING), 2, '0'))        AS month_label,
        claim_type,
        plan_type,
        metal_tier,
        COUNT(claim_id)                                          AS total_claims,
        COUNT(DISTINCT member_id)                                AS unique_members,
        SUM(billed_amount)                                       AS total_billed,
        SUM(allowed_amount)                                      AS total_allowed,
        SUM(paid_amount)                                         AS total_paid,
        SUM(member_oop)                                          AS total_member_oop,
        AVG(paid_amount)                                         AS avg_paid_per_claim,
        SUM(discount_amount)                                     AS total_discount,
        MIN(claim_date)                                          AS earliest_claim_date,
        MAX(claim_date)                                          AS latest_claim_date
    FROM `{CATALOG}`.`gold`.`fct_claims`
    WHERE claim_status = 'PAID'
    GROUP BY claim_year, claim_month, claim_type, plan_type, metal_tier
""")

print(f"Materialized view created: {CATALOG}.gold.mv_monthly_claims_summary")

# COMMAND ----------
# MAGIC %md ## Materialized View 2 — `mv_member_plan_spend`
# MAGIC **Refresh mode: INCREMENTAL**
# MAGIC
# MAGIC Per-member spend summary aggregated by plan, broken down by claim type.
# MAGIC Contains only `SUM` and `COUNT` aggregations on a single source table.
# MAGIC When a member's new claim is processed, only that member's row is re-aggregated.
# MAGIC
# MAGIC **Typical use:** Member cost transparency portals, care management tools,
# MAGIC high-utilisation member identification.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{CATALOG}`.`gold`.`mv_member_plan_spend`
    COMMENT 'Per-member lifetime spend by plan and claim type. Supports incremental refresh.'
    TBLPROPERTIES (
        'pipelines.channel'          = 'PREVIEW',
        'delta.enableChangeDataFeed' = 'true'
    )
    AS
    SELECT
        member_id,
        first_name,
        last_name,
        gender,
        age_band,
        state,
        plan_id,
        plan_name,
        plan_type,
        metal_tier,
        premium_monthly,
        COUNT(claim_id)                                              AS total_claims,
        COUNT(DISTINCT provider_id)                                  AS distinct_providers,
        SUM(billed_amount)                                           AS total_billed,
        SUM(paid_amount)                                             AS total_paid,
        SUM(member_oop)                                              AS total_member_oop,
        SUM(CASE WHEN claim_type = 'MEDICAL'   THEN paid_amount ELSE 0 END) AS medical_paid,
        SUM(CASE WHEN claim_type = 'PHARMACY'  THEN paid_amount ELSE 0 END) AS pharmacy_paid,
        SUM(CASE WHEN claim_type = 'DENTAL'    THEN paid_amount ELSE 0 END) AS dental_paid,
        SUM(CASE WHEN claim_status = 'PENDING' THEN billed_amount ELSE 0 END) AS pending_billed,
        MIN(claim_date)                                              AS first_claim_date,
        MAX(claim_date)                                              AS last_claim_date
    FROM `{CATALOG}`.`gold`.`fct_claims`
    GROUP BY
        member_id, first_name, last_name, gender, age_band, state,
        plan_id, plan_name, plan_type, metal_tier, premium_monthly
""")

print(f"Materialized view created: {CATALOG}.gold.mv_member_plan_spend")

# COMMAND ----------
# MAGIC %md ## Materialized View 3 — `mv_provider_rank_by_spend`
# MAGIC **Refresh mode: FULL (incremental NOT supported)**
# MAGIC
# MAGIC Ranks all providers by total paid claims spend using `RANK() OVER (ORDER BY ...)`.
# MAGIC Window functions compute a value relative to the entire result set — adding one
# MAGIC new provider row can change the rank of every other row, so there is no way to
# MAGIC apply just the changed rows; the full result must be recomputed.
# MAGIC
# MAGIC **Databricks behaviour:** When `REFRESH MATERIALIZED VIEW` is called, Databricks
# MAGIC detects the window function and automatically falls back to a full refresh,
# MAGIC ignoring CDF even if it is enabled on the source table.
# MAGIC
# MAGIC **Typical use:** Provider scorecards, network adequacy dashboards, contract reviews.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{CATALOG}`.`gold`.`mv_provider_rank_by_spend`
    COMMENT 'Provider performance with spend rank. FULL refresh only — contains RANK() window function.'
    TBLPROPERTIES (
        'pipelines.channel'          = 'PREVIEW',
        'delta.enableChangeDataFeed' = 'true'
    )
    AS
    SELECT
        provider_id,
        provider_name,
        provider_type,
        specialty,
        network_tier,
        is_in_network,
        total_claims,
        unique_members,
        total_billed,
        total_allowed,
        total_paid,
        avg_billed_per_claim,
        avg_paid_per_claim,
        discount_rate_pct,
        -- RANK() window function — this single line forces full refresh on every run
        RANK() OVER (ORDER BY total_paid DESC)                   AS spend_rank,
        RANK() OVER (
            PARTITION BY provider_type ORDER BY total_paid DESC
        )                                                        AS spend_rank_within_type,
        ROUND(total_paid / SUM(total_paid) OVER () * 100, 2)    AS pct_of_total_spend
    FROM `{CATALOG}`.`gold`.`rpt_provider_performance`
""")

print(f"Materialized view created: {CATALOG}.gold.mv_provider_rank_by_spend")

# COMMAND ----------
# MAGIC %md ## Trigger a Manual Refresh
# MAGIC
# MAGIC `REFRESH MATERIALIZED VIEW` applies the appropriate strategy automatically:
# MAGIC - Incremental-capable MVs use the CDF log — only changed rows are processed
# MAGIC - Full-refresh-only MVs recompute the entire result set
# MAGIC
# MAGIC Use `REFRESH MATERIALIZED VIEW ... FULL` to force a full refresh on any MV
# MAGIC regardless of incremental capability (useful for debugging or after schema changes).

# COMMAND ----------

for mv in ["mv_monthly_claims_summary", "mv_member_plan_spend", "mv_provider_rank_by_spend"]:
    print(f"Refreshing {CATALOG}.gold.{mv} ...")
    spark.sql(f"REFRESH MATERIALIZED VIEW `{CATALOG}`.`gold`.`{mv}`")
    print(f"  [OK] Refresh complete.")

# COMMAND ----------
# MAGIC %md ## Verify Incremental vs Full Refresh
# MAGIC
# MAGIC Three ways to confirm whether a refresh was incremental or full.

# COMMAND ----------
# MAGIC %md ### Method 1 — `DESCRIBE EXTENDED`
# MAGIC
# MAGIC Check the **`Is Incremental Capable`** property in the table details.
# MAGIC This is determined at creation time from the query shape — it tells you
# MAGIC whether the MV *can* refresh incrementally, not whether it *did* on the last run.
# MAGIC A value of `true` means Databricks will use CDF-based refresh whenever it runs.

# COMMAND ----------

for mv in ["mv_monthly_claims_summary", "mv_member_plan_spend", "mv_provider_rank_by_spend"]:
    print(f"\n{'='*60}")
    print(f"DESCRIBE EXTENDED: {mv}")
    print('='*60)
    desc = spark.sql(f"DESCRIBE EXTENDED `{CATALOG}`.`gold`.`{mv}`")
    # Filter to the rows most relevant for refresh mode inspection
    display(desc.filter(
        desc["col_name"].isin([
            "Is Incremental Capable",
            "Type",
            "Provider",
            "Comment",
            "Location",
        ]) |
        desc["col_name"].startswith("Table Properties")
    ))

# COMMAND ----------
# MAGIC %md ### Method 2 — `DESCRIBE HISTORY`
# MAGIC
# MAGIC After each refresh, a history entry is written to the Delta log.
# MAGIC The `operationParameters` column contains **`refreshMode`**:
# MAGIC - `"INCREMENTAL"` — only changed rows were processed
# MAGIC - `"FULL"` — entire result set was recomputed
# MAGIC
# MAGIC This is the definitive proof of what actually happened on each run.

# COMMAND ----------

for mv in ["mv_monthly_claims_summary", "mv_member_plan_spend", "mv_provider_rank_by_spend"]:
    print(f"\n{'='*60}")
    print(f"DESCRIBE HISTORY: {mv}")
    print('='*60)
    history = spark.sql(f"DESCRIBE HISTORY `{CATALOG}`.`gold`.`{mv}`")
    display(
        history.select(
            "version", "timestamp", "operation",
            "operationParameters", "operationMetrics"
        ).limit(5)
    )

# COMMAND ----------
# MAGIC %md ### Method 3 — `SHOW TBLPROPERTIES`
# MAGIC
# MAGIC Inspect the raw table properties set on the MV. Look for:
# MAGIC - `delta.enableChangeDataFeed` — source table CDF status
# MAGIC - Any Databricks-managed properties prefixed with `pipelines.` or `delta.`

# COMMAND ----------

for mv in ["mv_monthly_claims_summary", "mv_member_plan_spend", "mv_provider_rank_by_spend"]:
    print(f"\n{'='*60}")
    print(f"SHOW TBLPROPERTIES: {mv}")
    print('='*60)
    display(spark.sql(f"SHOW TBLPROPERTIES `{CATALOG}`.`gold`.`{mv}`"))

# COMMAND ----------
# MAGIC %md ### Method 4 — Query the MV Refresh Audit Log (System Tables)
# MAGIC
# MAGIC If your workspace has **system tables** enabled, you can query the refresh history
# MAGIC across all MVs in one place. This is the best approach for monitoring at scale.

# COMMAND ----------

# Requires system tables enabled on your Databricks workspace
# Replace 'your_workspace_id' or use the pre-configured system catalog
try:
    display(spark.sql(f"""
        SELECT
            entity_name,
            refresh_mode,
            status,
            start_time,
            end_time,
            ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 1) AS duration_seconds,
            error_message
        FROM system.query.materialized_views_refresh_history
        WHERE catalog_name = '{CATALOG}'
          AND schema_name  = 'gold'
        ORDER BY start_time DESC
        LIMIT 20
    """))
except Exception as e:
    print(f"[INFO] system.query.materialized_views_refresh_history not available: {e}")
    print("       Enable system tables in your workspace admin console to use this query.")

# COMMAND ----------
# MAGIC %md ## Quick Reference — Incremental vs Full Refresh
# MAGIC
# MAGIC | Query Pattern | Incremental? | Reason |
# MAGIC |---|---|---|
# MAGIC | `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` with `GROUP BY` | Yes | Pure aggregates can be maintained with delta rows |
# MAGIC | Simple `WHERE` filter | Yes | Filter applied before aggregation on changed rows |
# MAGIC | `INNER JOIN` / `LEFT JOIN` on equality | Yes | Equi-joins are CDF-compatible |
# MAGIC | `RANK()`, `ROW_NUMBER()`, `LAG()`, `LEAD()` | **No** | Global ordering changes on every new row |
# MAGIC | `COUNT(DISTINCT ...)` | **No** | Exact distinct counts require full re-scan |
# MAGIC | `SUM(DISTINCT ...)` | **No** | Same reason as COUNT DISTINCT |
# MAGIC | Non-deterministic functions (`RAND`, `UUID`) | **No** | Result changes on every evaluation |
# MAGIC | `LIMIT` / `OFFSET` | **No** | Row order is not stable across partial updates |
# MAGIC | Aggregate on top of aggregate (nested) | **No** | Cannot push incremental changes through two levels |
# MAGIC
# MAGIC **Source table requirements:**
# MAGIC - Must be a **Delta** table (not a view or external table)
# MAGIC - Must have `delta.enableChangeDataFeed = true` (set in Step 1 above)
# MAGIC - CDF must have been enabled **before** the first data was written, or a
# MAGIC   `REFRESH MATERIALIZED VIEW ... FULL` is needed to establish a baseline

# COMMAND ----------
# MAGIC %md ## Preview Materialized View Results

# COMMAND ----------

print("=== mv_monthly_claims_summary ===")
display(spark.sql(f"""
    SELECT month_label, claim_type, plan_type, total_claims,
           unique_members, total_billed, total_paid, avg_paid_per_claim
    FROM `{CATALOG}`.`gold`.`mv_monthly_claims_summary`
    ORDER BY month_label, total_paid DESC
"""))

print("=== mv_member_plan_spend ===")
display(spark.sql(f"""
    SELECT last_name, first_name, plan_name, plan_type,
           total_claims, distinct_providers, total_billed, total_paid, total_member_oop
    FROM `{CATALOG}`.`gold`.`mv_member_plan_spend`
    ORDER BY total_paid DESC
"""))

print("=== mv_provider_rank_by_spend ===")
display(spark.sql(f"""
    SELECT spend_rank, provider_name, provider_type, specialty,
           network_tier, total_claims, total_paid, discount_rate_pct,
           spend_rank_within_type, pct_of_total_spend
    FROM `{CATALOG}`.`gold`.`mv_provider_rank_by_spend`
    ORDER BY spend_rank
"""))
