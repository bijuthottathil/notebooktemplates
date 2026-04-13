# Databricks Medallion Architecture — Notebook Template Suite
## Alteryx → Databricks Migration | Batch Load Patterns

---

## Repository Structure

```
.
├── 00_environment/
│   ├── nb_env_01_storage_credentials.py       ← ADLS service principal credentials
│   ├── nb_env_02_external_locations.py        ← Unity Catalog external locations
│   └── nb_env_03_catalog_schema_setup.py      ← Catalog, schemas, audit tables
│
├── 01_bronze/                                 ← Raw ingestion (no transformation)
│   ├── nb_brz_01_full_load_batch.py           ← Pattern 1: Full Truncate & Reload
│   ├── nb_brz_02_incremental_watermark_batch.py ← Pattern 2: Incremental (watermark)
│   └── nb_brz_03_multiformat_file_landing.py  ← Pattern 3: Multi-format auto-detect
│
├── 02_silver/                                 ← Cleanse, conform, deduplicate
│   ├── nb_slv_01_cleanse_standardize.py       ← Pattern 1: Cleanse & Standardize
│   ├── nb_slv_02_scd_type2_batch.py           ← Pattern 2: SCD Type 2 History
│   ├── nb_slv_03_merge_upsert_batch.py        ← Pattern 3: Merge / Upsert
│   └── nb_slv_04_multisource_join_batch.py    ← Pattern 4: Multi-Source Join
│
├── 03_gold/                                   ← Business-ready marts
│   ├── nb_gld_01_fact_table_batch.py          ← Pattern 1: Fact Table Build
│   ├── nb_gld_02_dimension_table_batch.py     ← Pattern 2: Conformed Dimension
│   ├── nb_gld_03_report_mart_batch.py         ← Pattern 3: Pre-Aggregated Report Mart
│   ├── nb_gld_04_aggregation_mart_batch.py    ← Pattern 4: Rolling KPI Aggregations
│   └── nb_gld_05_data_product_export_batch.py ← Pattern 5: Data Product Export
│
└── 99_utilities/
    ├── nb_utils_data_quality.py               ← Reusable DQ check framework
    ├── nb_utils_audit_logger.py               ← Pipeline audit context manager
    └── nb_utils_workflow_orchestrator.py      ← Notebook chaining orchestrator
```

---

## Naming Convention

Inspired by Uber, Airbnb Minerva, and Netflix Data Platform practices.

| Component       | Convention                                      | Example                            |
|-----------------|------------------------------------------------|------------------------------------|
| **Notebook**    | `nb_<layer>_<seq>_<pattern>`                   | `nb_brz_01_full_load_batch`        |
| **Bronze table**| `<catalog>.bronze.<source>__<entity>`          | `company_dev.bronze.crm__customers`|
| **Silver table**| `<catalog>.silver.<domain>__<entity>`          | `company_dev.silver.crm__customers`|
| **SCD2 table**  | `<catalog>.silver.<domain>__<entity>_history`  | `company_dev.silver.crm__customers_history` |
| **Fact table**  | `<catalog>.gold.fct_<subject>`                 | `company_dev.gold.fct_sales_orders`|
| **Dimension**   | `<catalog>.gold.dim_<entity>`                  | `company_dev.gold.dim_customer`    |
| **Report mart** | `<catalog>.gold.rpt_<subject>_<grain>`         | `company_dev.gold.rpt_sales_daily` |
| **Aggregation** | `<catalog>.gold.agg_<subject>_<period>`        | `company_dev.gold.agg_sales_kpi_daily` |
| **Data product**| `<catalog>.gold.dp_<product_name>`             | `company_dev.gold.dp_sales_summary`|

Double underscore (`__`) between source and entity (dbt/Airbnb convention) signals a source boundary.

---

## 5 Batch Load Pattern Types

| # | Pattern              | Bronze Notebook          | Silver Notebook            | When to Use                                      |
|---|----------------------|--------------------------|----------------------------|--------------------------------------------------|
| 1 | Full Load            | `nb_brz_01_full_load`    | `nb_slv_01_cleanse`        | Source delivers complete snapshot every run      |
| 2 | Incremental Watermark| `nb_brz_02_incremental`  | `nb_slv_03_merge_upsert`   | Source has reliable updated_at / sequence column |
| 3 | Multi-Format Landing | `nb_brz_03_multiformat`  | `nb_slv_01_cleanse`        | Mixed CSV/Excel/JSON/Parquet in landing zone     |
| 4 | SCD Type 2           | `nb_brz_01_full_load`    | `nb_slv_02_scd_type2`      | Dimension history tracking required              |
| 5 | Multi-Source Join    | Multiple bronze notebooks| `nb_slv_04_multisource_join`| 360-view or enrichment from multiple systems    |

---

## Alteryx Tool → Databricks Mapping

| Alteryx Tool            | Databricks Equivalent                          |
|-------------------------|------------------------------------------------|
| Input Data (CSV/Excel)  | `nb_brz_01` / `nb_brz_03` (bronze ingestion)  |
| Formula Tool            | `withColumn()` + Spark SQL expressions         |
| Filter Tool             | `.filter()` / `WHERE` clause                   |
| Join Tool               | `.join()` / `nb_slv_04_multisource_join`       |
| Summarize Tool          | `.groupBy().agg()` / `nb_gld_03_report_mart`   |
| Running Total           | Window functions / `nb_gld_04_aggregation_mart`|
| Sort Tool               | `.orderBy()`                                   |
| Select Tool             | `.select()` / `.drop()`                        |
| Unique Tool             | `.dropDuplicates()`                            |
| Append Fields           | `.join(how='left')`                            |
| Output Data (CSV/Excel) | `nb_gld_05_data_product_export`                |
| Find Replace            | `F.regexp_replace()` / `F.when()`              |
| Multi-Row Formula       | Window functions (`LAG`, `ROW_NUMBER`)         |
| Dynamic Replace         | `nb_slv_02_scd_type2` (SCD2 MERGE)            |

---

## Setup Order (One-Time)

```
1. nb_env_01_storage_credentials   →  Register ADLS service principal
2. nb_env_02_external_locations    →  Create Unity Catalog external locations
3. nb_env_03_catalog_schema_setup  →  Create catalog, schemas, audit tables
```

## Daily Batch Pipeline Order

```
Bronze (ingest)  →  Silver (cleanse/conform)  →  Gold (aggregate/publish)
```

---

## Cluster Requirements

| Library                          | Purpose                   | Install                              |
|----------------------------------|---------------------------|--------------------------------------|
| `com.crealytics:spark-excel`     | Excel (.xlsx) ingestion   | Maven: `com.crealytics:spark-excel_2.12:3.5.1_0.20.3` |
| `org.apache.spark:spark-avro`    | Avro ingestion            | Built-in on DBR 12+                  |
| `delta-spark`                    | Delta Lake                | Built-in on DBR 12+                  |
| `python-dateutil`                | Date arithmetic in gold   | Built-in                             |

**Minimum Databricks Runtime:** 13.3 LTS (includes Delta 3.x, Unity Catalog support)

---

## Metadata Columns Reference

### Bronze (`_` prefix — pipeline internal)
| Column              | Description                          |
|---------------------|--------------------------------------|
| `_ingest_id`        | UUID for this pipeline run           |
| `_ingest_ts`        | Timestamp loaded to bronze           |
| `_ingest_date`      | Partition key (date)                 |
| `_source_system`    | Source system identifier             |
| `_source_entity`    | Entity/table name                    |
| `_source_file_path` | ADLS file path ingested from         |
| `_source_format`    | csv / excel / json / parquet / orc   |
| `_load_type`        | full / incremental / file_landing    |
| `_batch_date`       | Business date of this batch          |

### Silver (additional columns)
| Column                | Description                        |
|-----------------------|------------------------------------|
| `_silver_ingest_id`   | Silver pipeline run UUID           |
| `_silver_ingest_ts`   | Timestamp cleansed to silver       |
| `_silver_batch_date`  | Silver partition key               |
| `_is_current`         | True for SCD2 current record       |
| `_effective_from`     | SCD2 version start date            |
| `_effective_to`       | SCD2 version end date (NULL=active)|
| `_record_hash`        | MD5 of tracked SCD2 columns        |

### Gold (additional columns)
| Column               | Description                         |
|----------------------|-------------------------------------|
| `_gold_ingest_id`    | Gold pipeline run UUID              |
| `_gold_ingest_ts`    | Timestamp published to gold         |
| `_gold_batch_date`   | Gold partition key                  |
| `<entity>_sk`        | MD5 surrogate key for BI joins      |
