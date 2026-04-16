# Notebook Templates — Databricks Medallion Architecture

Reusable Databricks notebook templates and end-to-end ETL pipeline examples for
migrating batch workloads from Alteryx to Databricks using the medallion architecture
(Bronze → Silver → Gold) with Unity Catalog and ADLS Gen2.

---

## Repository Structure

```
├── Template1/          Parameterised templates using dbutils.widgets
├── Template2/          Simplified templates using plain Python config variables
├── etl-template1/      End-to-end health insurance pipeline example  ← detailed below
└── etl-template2/      End-to-end supply chain pipeline example
```

---

## etl-template1 — Health Insurance Pipeline

A fully runnable, end-to-end Databricks medallion pipeline modelled on a health
insurance domain (Humana-style). Covers plan, member, provider, and claims data
through Bronze → Silver → Gold layers with audit logging, DQ checks, Spark
performance tuning, materialized views, and incremental load testing.

### Folder Layout

```
etl-template1/
├── .env                             Credential configuration (replaces dbutils.widgets)
├── nb_00_setup_and_load_data.py     One-time infrastructure setup + sample data upload
├── nb_01_bronze_ingest.py           CSV landing zone → bronze Delta tables
├── nb_02_silver_cleanse.py          Bronze → silver cleanse, enrich, conformed join
├── nb_03_gold_mart.py               Silver → four gold tables + DQ checks
├── nb_04_run_pipeline.py            Single-click orchestrator (nb_01 → nb_02 → nb_03)
├── nb_05_materialized_views.py      Three gold materialized views + refresh verification
├── data/                            Initial full-load CSV sample data
│   ├── plans.csv                    4 insurance plans (PPO/HMO/EPO, metal tiers)
│   ├── members.csv                  15 enrolled members across 12 US states
│   ├── providers.csv                8 providers (PCPs, specialists, pharmacy)
│   └── claims.csv                   52 claims — medical, pharmacy, dental (Q1–Q3 2024)
├── data_incremental/                Second-batch load for incremental testing
│   ├── plans.csv                    Same 4 plans + 1 new (PL005 Catastrophic EPO)
│   ├── members.csv                  2 new members + 3 existing with updates
│   ├── providers.csv                Same 8 providers + 1 new (PRV009 Psychiatry)
│   └── claims.csv                   13 new Aug 2024 claims + 4 resolved PENDING/DENIED
└── utilities/
    ├── nb_utils_audit_logger.py     PipelineAudit context manager + query helpers
    └── nb_utils_data_quality.py     run_dq_suite() — 8 check types, writes to audit table
```

---

### Credential Configuration — `.env` File

This pipeline replaces `dbutils.widgets.text()` calls with a `.env` file, loaded
via `python-dotenv` in `nb_00`. This is the only notebook that needs credentials
directly — downstream notebooks (`nb_01`–`nb_04`) access data through Unity Catalog
and do not require the service principal directly.

**`.env` keys:**

| Key | Default | Description |
|---|---|---|
| `CREDENTIAL_NAME` | `adls_sp_credential` | Name for the Unity Catalog storage credential |
| `CLIENT_ID_SECRET_SCOPE` | `kv-databricks` | Databricks secret scope (Key Vault-backed) |
| `CLIENT_ID_SECRET_KEY` | `sp-client-id` | Secret key holding the service principal app ID |
| `CLIENT_SECRET_KEY` | `sp-client-secret` | Secret key holding the SP client secret |
| `TENANT_ID_SECRET_KEY` | `sp-tenant-id` | Secret key holding the AAD tenant ID |

Edit `.env` before running `nb_00`. Do **not** commit real secrets — add `.env` to
`.gitignore` in production.

`.env` is resolved at runtime using the Databricks Repos notebook context:
```python
_nb_dir  = "/Workspace" + "/".join(_nb_path.split("/")[:-1])
load_dotenv(os.path.join(_nb_dir, ".env"))
```
This works in any workspace where the repo is cloned without hardcoded paths.

---

### Data Model

#### Source (Landing Zone → Bronze)

| Entity | Rows (initial) | Key Columns |
|---|---|---|
| `plans` | 4 | plan_id, plan_type, metal_tier, annual_deductible, oop_max, premium_monthly |
| `members` | 15 | member_id, plan_id, dob, gender, state, enrollment_date, termination_date |
| `providers` | 8 | provider_id, provider_type, specialty, npi_number, network_tier, is_in_network |
| `claims` | 52 | claim_id, member_id, provider_id, claim_type, diagnosis_code, procedure_code, amounts, status |

#### Silver Tables (Cleansed + Conformed)

| Table | Description |
|---|---|
| `silver.plans` | Type-cast, trimmed, derived `is_active` flag |
| `silver.members` | Type-cast, derived `age` and `age_band` (0-17 / 18-34 / 35-49 / 50-64 / 65+) |
| `silver.providers` | Type-cast, NPI validated (10-digit regex in DQ suite) |
| `silver.claims` | Type-cast, derived `member_oop`, `discount_amount`, `claim_year`, `claim_month`; partitioned by year/month |
| `silver.claims_enriched` | Four-way conformed join: claims + members + providers + plans. Grain: one row per claim. Broadcast joins used for providers (8 rows) and plans (4 rows) |

#### Gold Tables

| Table | Grain | Description |
|---|---|---|
| `gold.fct_claims` | One row per claim | Full enriched fact; partitioned by `claim_year`/`claim_month`; MD5 surrogate key `fct_claim_sk` |
| `gold.rpt_member_utilization` | One row per member | Total spend, distinct providers, spend by claim type, utilisation rank (window) |
| `gold.rpt_provider_performance` | One row per provider | Volume, avg cost, discount rate, total paid |
| `gold.rpt_spend_by_plan` | One row per plan × claim type × month | Monthly spend with MoM % change (LAG window); partitioned by year/month |

---

### Pipeline Notebooks

#### `nb_00_setup_and_load_data.py` — One-time Setup

Sequence:
1. Install `python-dotenv` and load `.env`
2. Create Unity Catalog **storage credential** from Key Vault secrets
3. Create two **external locations** (`ext_loc_hi_landing`, `ext_loc_hi_catalog`)
4. Validate read/write access with probe files
5. Create **catalog** `health_insurance_dev` with `MANAGED LOCATION` on ADLS
6. Create schemas: `bronze`, `silver`, `gold`, `audit`
7. Create audit tables: `pipeline_run_log` (16 columns), `dq_check_results` (12 columns)
8. Generate sample data as Spark DataFrames and upload to ADLS landing zone
   *(alternatively, upload CSV files from `data/` folder manually)*

#### `nb_01_bronze_ingest.py` — Bronze Layer

- Reads four CSVs from ADLS landing zone using `spark.read.csv` with `inferSchema`
- Adds metadata columns: `_ingest_id`, `_ingest_ts`, `_batch_date`, `_source_file`
- Writes to `bronze.raw_*` Delta tables via `saveAsTable` (Unity Catalog managed)
- Wrapped in `PipelineAudit` context manager — logs RUNNING / SUCCESS / FAILED
- Runs `OPTIMIZE ZORDER BY` on all four bronze tables after write
- DQ suites per table: not_null on keys, unique on keys, accepted_values for enums,
  NPI 10-digit regex check on providers

#### `nb_02_silver_cleanse.py` — Silver Layer

Each entity cleansed in its own `PipelineAudit` block:

| Entity | Key transforms | DQ checks |
|---|---|---|
| `plans` | Cast amounts, derive `is_active` | range on deductible/oop/premium, valid plan_type |
| `members` | Cast dates, derive `age` + `age_band` | range on age (0–120) |
| `providers` | Cast boolean, trim strings | NPI 10-digit regex |
| `claims` | Cast dates + amounts, derive `member_oop`, `discount_amount`, `claim_year/month` | range on amounts, valid claim_status |
| `claims_enriched` | Four-way join with broadcast hints | referential integrity (last_name, provider_name, plan_name not null) |

Broadcast joins: `providers` (8 rows) and `plans` (4 rows) are wrapped in
`F.broadcast()` — zero shuffle cost for those joins.

`claims_enriched` is `.cache()`-d before `saveAsTable`, then the post-write
`count()` is served from the in-memory plan. `unpersist()` called immediately after.

`OPTIMIZE ZORDER BY` run on all five silver tables.

#### `nb_03_gold_mart.py` — Gold Layer

`silver.claims_enriched` is cached once and shared across all four gold table builds:

```python
silver = spark.table(SOURCE)
silver.cache()
source_rows = silver.count()   # materialise
```

`silver.unpersist()` is called after the last gold table is written.

Each gold table is built in its own `PipelineAudit` block. `fct_claims` and
`rpt_spend_by_plan` are partitioned by `claim_year`/`claim_month`. All gold tables
have `OPTIMIZE ZORDER BY` applied after write.

DQ suite on `fct_claims`: unique surrogate key, not_null on keys, range on amounts,
valid claim_status.

#### `nb_04_run_pipeline.py` — Orchestrator

Chains `nb_01 → nb_02 → nb_03` via `dbutils.notebook.run()` with 600s timeout.
After all steps complete:
- Calls `show_pipeline_history(catalog=CATALOG, days=1)` from the audit utility
- Displays `audit.dq_check_results` for the current batch

---

### Materialized Views (`nb_05_materialized_views.py`)

Three gold materialized views demonstrating incremental vs full refresh behaviour.

#### Prerequisites

Change Data Feed must be enabled on all source gold tables before incremental refresh
will work. `nb_05` handles this automatically:

```python
ALTER TABLE gold.fct_claims SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

#### View Definitions

**`gold.mv_monthly_claims_summary`** — Incremental refresh
```sql
SELECT claim_year, claim_month, claim_type, plan_type, metal_tier,
       COUNT(claim_id), COUNT(DISTINCT member_id),
       SUM(billed_amount), SUM(paid_amount), AVG(paid_amount), ...
FROM gold.fct_claims
WHERE claim_status = 'PAID'
GROUP BY claim_year, claim_month, claim_type, plan_type, metal_tier
```
*Supports incremental refresh — pure `SUM/COUNT/AVG GROUP BY`, no window functions.*

**`gold.mv_member_plan_spend`** — Incremental refresh
```sql
SELECT member_id, plan_id, plan_type,
       COUNT(claim_id), SUM(paid_amount),
       SUM(CASE WHEN claim_type = 'MEDICAL' THEN paid_amount ELSE 0 END), ...
FROM gold.fct_claims
GROUP BY member_id, first_name, last_name, gender, age_band, state,
         plan_id, plan_name, plan_type, metal_tier, premium_monthly
```
*Supports incremental refresh — single-source `SUM/COUNT GROUP BY`, no window functions.*

**`gold.mv_provider_rank_by_spend`** — Full refresh only
```sql
SELECT provider_id, ...,
       RANK() OVER (ORDER BY total_paid DESC)                AS spend_rank,
       RANK() OVER (PARTITION BY provider_type ORDER BY ...) AS spend_rank_within_type,
       ROUND(total_paid / SUM(total_paid) OVER () * 100, 2) AS pct_of_total_spend
FROM gold.rpt_provider_performance
```
*Full refresh only — `RANK()` and `SUM() OVER ()` window functions require the full
result set to be recomputed on every run.*

#### Why certain queries cannot refresh incrementally

| Query pattern | Incremental? | Reason |
|---|---|---|
| `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` + `GROUP BY` | Yes | Aggregates can be updated with delta rows only |
| Simple `WHERE` filter | Yes | Applied on changed rows before aggregation |
| `INNER JOIN` / `LEFT JOIN` on equality | Yes | CDF-compatible join strategy |
| `RANK()`, `ROW_NUMBER()`, `LAG()`, `LEAD()` | **No** | Adding one row can change every other row's rank |
| `COUNT(DISTINCT ...)` | **No** | Exact distinct counts require a full re-scan |
| Non-deterministic functions (`RAND`, `UUID`) | **No** | Result changes on every evaluation |
| `LIMIT` / `OFFSET` | **No** | Row order is unstable across partial updates |
| Nested aggregations | **No** | Cannot push incremental changes through two aggregation levels |

#### Verifying Refresh Mode — Four Methods

**Method 1 — `DESCRIBE EXTENDED`**

Checks whether the MV *can* refresh incrementally (determined at creation time from
query shape). Look for the `Is Incremental Capable` property.

```sql
DESCRIBE EXTENDED health_insurance_dev.gold.mv_monthly_claims_summary;
```

**Method 2 — `DESCRIBE HISTORY`** *(definitive — what actually ran)*

After each `REFRESH MATERIALIZED VIEW`, a history entry is written to the Delta log.
`operationParameters.refreshMode` will be `"INCREMENTAL"` or `"FULL"`.

```sql
DESCRIBE HISTORY health_insurance_dev.gold.mv_monthly_claims_summary;
-- Look for: operationParameters → refreshMode
```

**Method 3 — `SHOW TBLPROPERTIES`**

Inspect raw table properties including `delta.enableChangeDataFeed` and any
Databricks-managed `pipelines.*` properties.

```sql
SHOW TBLPROPERTIES health_insurance_dev.gold.mv_monthly_claims_summary;
```

**Method 4 — System Tables** *(workspace-level monitoring)*

Requires system tables enabled in the workspace admin console.

```sql
SELECT entity_name, refresh_mode, status, start_time, end_time,
       ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 1) AS duration_seconds
FROM system.query.materialized_views_refresh_history
WHERE catalog_name = 'health_insurance_dev'
  AND schema_name  = 'gold'
ORDER BY start_time DESC;
```

**Force a full refresh** (useful after schema changes or for debugging):
```sql
REFRESH MATERIALIZED VIEW health_insurance_dev.gold.mv_monthly_claims_summary FULL;
```

---

### Incremental Data — `data_incremental/`

A second-batch dataset for testing upsert/merge behaviour and MV incremental refresh.

**Re-loaded existing records** (same IDs, updated values):

| Record | Change |
|---|---|
| CLM039 | `PENDING → PAID` — Sofia's cardiology echo, $2,320 paid after processing |
| CLM040 | `PENDING → PAID` — Priya's endocrinology visit resolved |
| CLM041 | `PENDING → PAID` — Aisha's knee replacement, $8,000 paid (high deductible applied) |
| CLM042 | `DENIED → PAID` — Derek's urgent care, appeal approved |
| MBR014 | Termination cleared, plan changed PL002 → PL001, `is_active = true` (re-activated) |

**New records:**

| File | What's new |
|---|---|
| `plans.csv` | PL005 Catastrophic EPO — new plan tier, effective 2024-07-01 |
| `members.csv` | MBR016 Grace Nguyen (PL005, TX) and MBR017 Andre Washington (PL002, NY) |
| `providers.csv` | PRV009 Lakeside Mental Health — Psychiatry, Tier 1, NY |
| `claims.csv` | CLM053–CLM065 — 13 August 2024 claims including first activity for MBR016, MBR017, PRV009 |

**What this tests for MV incremental refresh:**

After running the pipeline with `data_incremental/` data:
1. CDF on `gold.fct_claims` records the changed/new rows
2. `REFRESH MATERIALIZED VIEW mv_monthly_claims_summary` re-aggregates only the
   affected months (May 2024 for the 4 resolved claims, August 2024 for the new ones)
3. `REFRESH MATERIALIZED VIEW mv_provider_rank_by_spend` does a full recompute
   because `RANK()` must recalculate globally — confirm this via `DESCRIBE HISTORY`

---

### Utilities

Both utility notebooks are in `etl-template1/utilities/` and imported with `%run`:

```python
# MAGIC %run ./utilities/nb_utils_audit_logger
# MAGIC %run ./utilities/nb_utils_data_quality
```

#### `nb_utils_audit_logger.py`

**`PipelineAudit`** — context manager used in every pipeline notebook:

```python
with PipelineAudit(catalog=CATALOG, pipeline_name="nb_01_bronze_ingest",
                   layer="bronze", source_system="health_insurance",
                   entity_name="claims", batch_date=BATCH_DATE,
                   load_type="full") as audit:
    audit.set_rows_read(n)
    audit.set_rows_written(n)
```

Writes `RUNNING` on entry, `SUCCESS` or `FAILED` (with error message) on exit via
`MERGE INTO audit.pipeline_run_log`. Captures notebook path automatically.

**Query helpers:**
- `show_pipeline_history(catalog, pipeline_name=None, days=7)` — recent run summary
- `show_failed_runs(catalog, days=1)` — failed runs for alerting/monitoring

**`audit.pipeline_run_log` schema:**
`run_id`, `pipeline_name`, `layer`, `source_system`, `entity_name`, `batch_date`,
`load_type`, `status`, `rows_read`, `rows_written`, `rows_rejected`,
`start_ts`, `end_ts`, `duration_seconds`, `error_message`, `notebook_path`

#### `nb_utils_data_quality.py`

**`run_dq_suite(df, suite, entity_name, layer, run_id, catalog, batch_date)`**

Runs a list of typed DQ checks and appends results to `audit.dq_check_results`.
Raises `RuntimeError` on any check with `threshold=1.0` that fails.

| Check type | What it validates |
|---|---|
| `not_null` | Column has no NULL values |
| `unique` | Column (or column set) has no duplicates |
| `not_empty_string` | No blank/whitespace-only strings |
| `range` | Values fall within `min` / `max` bounds |
| `regex` | Values match a regular expression pattern |
| `accepted_values` | Values are within an allowed list |
| `row_count_threshold` | Row count is at least `min_rows` |
| `freshness` | Timestamps are within `max_age_hours` of now |

---

### Spark Performance Tuning

Applied consistently across `nb_01`, `nb_02`, `nb_03`:

| Setting | Value | Applied in |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | All notebooks — AQE re-plans at runtime |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | All notebooks — merges small shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | nb_02, nb_03 — splits skewed join partitions |
| `spark.sql.shuffle.partitions` | `8` | nb_02, nb_03 — right-sized for this dataset |
| `spark.sql.autoBroadcastJoinThreshold` | `20 MB` | nb_02 — plans (4 rows) and providers (8 rows) broadcast |
| `spark.sql.files.maxPartitionBytes` | `128 MB` | nb_01 — partition size when reading CSV |
| `spark.sql.windowExec.buffer.spill.threshold` | `4096` | nb_03 — limits in-memory rows per window partition |
| `delta.optimizeWrite.enabled` | `true` | All notebooks — auto right-sizes Parquet files on write |
| `delta.autoCompact.enabled` | `true` | All notebooks — compacts Delta files after writes |
| `OPTIMIZE ZORDER BY` | Per table | All layers — co-locates rows by join/filter keys |
| `.cache()` / `.unpersist()` | `claims_enriched`, `silver` | nb_02, nb_03 — avoids recomputing multi-way joins |
| `F.broadcast()` | providers, plans | nb_02 — explicit hint on small dimension tables |

---

### Infrastructure

| Resource | Name | Purpose |
|---|---|---|
| ADLS Storage Account | `dlshealthdev` | All raw and Delta data |
| Container | `datalake` | Single container |
| ADLS Base Path | `abfss://datalake@dlshealthdev.dfs.core.windows.net/etl_template1` | Root for all data |
| Landing Path | `.../etl_template1/landing/` | CSV file drops |
| Catalog Path | `.../etl_template1/catalog/` | Delta managed tables |
| Storage Credential | `sc_adls_credential` (from `.env`) | Unity Catalog → ADLS access |
| External Location (landing) | `ext_loc_hi_landing` | Landing zone governance |
| External Location (catalog) | `ext_loc_hi_catalog` | Catalog managed location |
| Unity Catalog | `health_insurance_dev` | Managed location → ADLS catalog path |

---

### Run Order

```
1. nb_00_setup_and_load_data   (once — creates infra and uploads initial data)
2. nb_04_run_pipeline          (runs nb_01 → nb_02 → nb_03 in sequence)
3. nb_05_materialized_views    (once to create MVs; re-run to refresh)

For incremental batch:
4. Upload data_incremental/ CSVs to ADLS landing zone
5. nb_04_run_pipeline          (pipeline handles upsert/merge automatically)
6. REFRESH MATERIALIZED VIEW   (run inside nb_05 or ad-hoc in a SQL cell)
```
