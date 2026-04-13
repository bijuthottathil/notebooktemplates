# Databricks notebook source
# MAGIC %md
# MAGIC # nb_00_setup_and_load_data
# MAGIC **Purpose:** One-time infrastructure setup — creates storage credentials, external locations,
# MAGIC Unity Catalog, schemas, audit table, and uploads sample CSV files to the ADLS landing zone.
# MAGIC
# MAGIC **Run this notebook once before running any other notebook in this pipeline.**
# MAGIC
# MAGIC **Setup sequence:**
# MAGIC ```
# MAGIC 1. Storage Credential   (service principal → Unity Catalog)
# MAGIC 2. External Locations   (ADLS paths → Unity Catalog)
# MAGIC 3. Catalog              (with MANAGED LOCATION on ADLS)
# MAGIC 4. Schemas              (bronze / silver / gold / audit)
# MAGIC 5. Audit Tables         (pipeline_run_log, dq_check_results)
# MAGIC 6. Generate & Upload CSV sample data → ADLS landing zone
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## Configuration
# MAGIC Edit this section before running. All other cells derive their values from here.

# COMMAND ----------

# ── ADLS Gen2 ─────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT  = "dlscompanydev"          # ADLS Gen2 storage account name
CONTAINER        = "datalake"               # Container name
ADLS_BASE        = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template2"

# ── Azure Key Vault-backed secret scope ───────────────────────────────────────
SECRET_SCOPE     = "kv-databricks"          # Databricks secret scope name
CLIENT_ID_KEY    = "sp-client-id"           # Secret key holding the service principal app ID
CLIENT_SECRET_KEY= "sp-client-secret"       # Secret key holding the service principal secret
TENANT_ID_KEY    = "sp-tenant-id"           # Secret key holding the AAD tenant ID

# ── Unity Catalog ─────────────────────────────────────────────────────────────
CREDENTIAL_NAME  = "sc_adls_credential"     # Name for the storage credential in Unity Catalog
CATALOG          = "supply_chain_dev"       # Catalog to create

# ── Derived paths (do not edit) ───────────────────────────────────────────────
LANDING_PATH     = f"{ADLS_BASE}/landing"
CATALOG_PATH     = f"{ADLS_BASE}/catalog"   # Catalog managed location (Delta tables)

# External location names
EXT_LOC_LANDING  = "ext_loc_sc_landing"
EXT_LOC_CATALOG  = "ext_loc_sc_catalog"

# ─────────────────────────────────────────────────────────────────────────────

print(f"Storage Account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print(f"ADLS Base       : {ADLS_BASE}")
print(f"Landing Path    : {LANDING_PATH}")
print(f"Catalog Path    : {CATALOG_PATH}")
print(f"Credential Name : {CREDENTIAL_NAME}")
print(f"Catalog         : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## 1. Create Storage Credential
# MAGIC
# MAGIC Registers an Azure service principal as a Unity Catalog **storage credential**.
# MAGIC This credential is then referenced by external locations to grant Databricks access
# MAGIC to the ADLS Gen2 container. Requires **account admin** or **credential admin** privilege.

# COMMAND ----------

# Retrieve secrets from Azure Key Vault-backed scope
client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_ID_KEY)
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_SECRET_KEY)
tenant_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=TENANT_ID_KEY)

print("Secrets retrieved from Key Vault scope.")

# COMMAND ----------

spark.sql(f"""
    CREATE STORAGE CREDENTIAL IF NOT EXISTS `{CREDENTIAL_NAME}`
    WITH AZURE_SERVICE_CREDENTIAL (
        DIRECTORY_ID = '{tenant_id}',
        APPLICATION_ID = '{client_id}',
        CLIENT_SECRET = '{client_secret}'
    )
    COMMENT 'Service principal credential for ADLS Gen2 – etl_template2 demo'
""")

print(f"Storage credential ready: {CREDENTIAL_NAME}")
spark.sql(f"DESCRIBE STORAGE CREDENTIAL `{CREDENTIAL_NAME}`").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 2. Create External Locations
# MAGIC
# MAGIC External locations bind an ADLS path to the storage credential so Spark can
# MAGIC read/write at that path through Unity Catalog governance.
# MAGIC
# MAGIC | External Location     | ADLS Path                              | Used for                     |
# MAGIC |-----------------------|----------------------------------------|------------------------------|
# MAGIC | ext_loc_sc_landing    | `.../etl_template2/landing/`           | CSV file drops (read only)   |
# MAGIC | ext_loc_sc_catalog    | `.../etl_template2/catalog/`           | Catalog managed Delta tables |

# COMMAND ----------

for loc_name, url in {
    EXT_LOC_LANDING: f"{LANDING_PATH}/",
    EXT_LOC_CATALOG: f"{CATALOG_PATH}/",
}.items():
    try:
        spark.sql(f"""
            CREATE EXTERNAL LOCATION IF NOT EXISTS `{loc_name}`
            URL '{url}'
            WITH (STORAGE CREDENTIAL `{CREDENTIAL_NAME}`)
            COMMENT 'etl_template2 demo – managed by nb_00'
        """)
        print(f"  [OK] {loc_name} → {url}")
    except Exception as e:
        print(f"  [WARN] {loc_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 3. Validate External Location Access
# MAGIC
# MAGIC Writes a small probe file to each external location and reads it back.
# MAGIC Both must show PASS before proceeding.

# COMMAND ----------

import datetime
from pyspark.sql import Row

def validate_location(loc_name, url):
    probe = f"{url.rstrip('/')}_probe/probe.json"
    try:
        (spark.createDataFrame([Row(test="ok", ts=str(datetime.datetime.utcnow()))])
             .write.mode("overwrite").json(probe))
        spark.read.json(probe).count()
        dbutils.fs.rm(probe, recurse=True)
        print(f"  [PASS] {loc_name}")
        return True
    except Exception as e:
        print(f"  [FAIL] {loc_name}: {e}")
        return False

results = {
    EXT_LOC_LANDING: validate_location(EXT_LOC_LANDING, f"{LANDING_PATH}/"),
    EXT_LOC_CATALOG: validate_location(EXT_LOC_CATALOG, f"{CATALOG_PATH}/"),
}

if not all(results.values()):
    raise RuntimeError("One or more external locations failed access validation. Check credential and ADLS permissions.")

print("\nAll external locations validated.")

# COMMAND ----------
# MAGIC %md ## 4. Create Unity Catalog with Managed Location
# MAGIC
# MAGIC Setting `MANAGED LOCATION` tells Unity Catalog to store all managed Delta tables
# MAGIC created inside this catalog (bronze, silver, gold schemas) in the ADLS path
# MAGIC `ext_loc_sc_catalog`. No explicit storage path is needed in `saveAsTable` calls.

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG}`
    MANAGED LOCATION '{CATALOG_PATH}/'
    COMMENT 'Supply chain ETL demo – managed tables stored on ADLS via ext_loc_sc_catalog'
""")
spark.sql(f"USE CATALOG `{CATALOG}`")

print(f"Catalog ready: {CATALOG}")
print(f"  Managed location: {CATALOG_PATH}/")

# COMMAND ----------
# MAGIC %md ## 5. Create Schemas

# COMMAND ----------

for schema, comment in {
    "bronze": "Raw ingested CSV data — no transformations",
    "silver": "Cleansed and conformed data",
    "gold":   "Business aggregations and KPI marts",
    "audit":  "Pipeline run logs and DQ check results",
}.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`
        COMMENT '{comment}'
    """)
    print(f"  Schema ready: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md ## 6. Create Audit Tables
# MAGIC
# MAGIC Two audit tables are created:
# MAGIC - `pipeline_run_log` — one row per pipeline run (written by `PipelineAudit` context manager)
# MAGIC - `dq_check_results` — one row per DQ check (written by `run_dq_suite`)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`audit`.`pipeline_run_log` (
        run_id           STRING,
        pipeline_name    STRING,
        layer            STRING,
        source_system    STRING,
        entity_name      STRING,
        batch_date       STRING,
        load_type        STRING,
        status           STRING,
        rows_read        BIGINT,
        rows_written     BIGINT,
        rows_rejected    BIGINT,
        start_ts         TIMESTAMP,
        end_ts           TIMESTAMP,
        duration_seconds DOUBLE,
        error_message    STRING,
        notebook_path    STRING
    )
    USING DELTA
    COMMENT 'Pipeline run log – written by PipelineAudit context manager'
""")
print(f"Audit table ready: {CATALOG}.audit.pipeline_run_log")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`audit`.`dq_check_results` (
        run_id        STRING,
        check_name    STRING,
        entity_name   STRING,
        layer         STRING,
        check_type    STRING,
        column_name   STRING,
        rows_checked  BIGINT,
        rows_failed   BIGINT,
        pass_rate     DOUBLE,
        threshold     DOUBLE,
        result        STRING,
        batch_date    STRING
    )
    USING DELTA
    COMMENT 'DQ check results – written by run_dq_suite utility'
""")
print(f"Audit table ready: {CATALOG}.audit.dq_check_results")

# COMMAND ----------
# MAGIC %md ## 7. Generate Sample Data & Upload to ADLS Landing Zone
# MAGIC
# MAGIC Creates the four supply chain CSV datasets in Spark and writes them directly
# MAGIC to the ADLS landing path using the external location registered above.
# MAGIC
# MAGIC **Alternatively:** manually upload the CSV files from the `data/` folder in this
# MAGIC repo to `{LANDING_PATH}/<entity>/` on your ADLS container.

# COMMAND ----------

from pyspark.sql import functions as F

def write_csv_to_adls(df, entity_name):
    """Write a DataFrame as a single CSV file to the ADLS landing zone."""
    path = f"{LANDING_PATH}/{entity_name}/"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  [OK] {entity_name} → {path}  ({df.count()} rows)")

# ── Suppliers ──────────────────────────────────────────────────────────────────
suppliers_df = spark.createDataFrame([
    ("SUP001","Acme Industrial",    "USA",         "North America", "orders@acme-ind.com",       "NET30", 4.5, "true"),
    ("SUP002","GlobalParts Ltd",    "Germany",     "Europe",        "supply@globalparts.de",     "NET45", 4.8, "true"),
    ("SUP003","FastFix Co",         "China",       "Asia Pacific",  "sales@fastfix.cn",          "NET60", 3.9, "true"),
    ("SUP004","SteelWorks Inc",     "Canada",      "North America", "procurement@steelworks.ca", "NET30", 4.2, "true"),
    ("SUP005","TechComponents AG",  "Switzerland", "Europe",        "orders@techcomp.ch",        "NET45", 4.7, "true"),
    ("SUP006","Pacific Supply",     "Australia",   "Asia Pacific",  "info@pacificsupply.au",     "NET30", 4.1, "true"),
    ("SUP007","Nordic Materials",   "Sweden",      "Europe",        "sales@nordicmat.se",        "NET60", 4.6, "true"),
    ("SUP008","Southern Metals",    "Mexico",      "Latin America", "compras@southernmetals.mx", "NET30", 3.7, "true"),
    ("SUP009","EastWest Trading",   "India",       "Asia Pacific",  "trade@ewtrading.in",        "NET45", 4.0, "false"),
    ("SUP010","PrecisionParts USA", "USA",         "North America", "orders@precisionparts.com", "NET30", 4.9, "true"),
], ["supplier_id","supplier_name","country","region","contact_email","payment_terms","rating","is_active"])

write_csv_to_adls(suppliers_df, "suppliers")

# ── Products ───────────────────────────────────────────────────────────────────
products_df = spark.createDataFrame([
    ("PRD001","Steel Bolt M10",        "Fasteners",       "Bolts",        0.15,  0.45,  "Each",  7,  500,  "true"),
    ("PRD002","Steel Nut M10",         "Fasteners",       "Nuts",         0.10,  0.30,  "Each",  7,  500,  "true"),
    ("PRD003","Stainless Washer 10mm", "Fasteners",       "Washers",      0.05,  0.18,  "Each",  7,  1000, "true"),
    ("PRD004","Hydraulic Hose 1/2in",  "Hydraulics",      "Hoses",        12.50, 28.00, "Metre", 14, 50,   "true"),
    ("PRD005","Hydraulic Fitting 90deg","Hydraulics",     "Fittings",     3.75,  9.50,  "Each",  14, 100,  "true"),
    ("PRD006","Bearing 6205-2RS",      "Bearings",        "Ball Bearings",4.20,  11.00, "Each",  10, 200,  "true"),
    ("PRD007","Bearing 6305-ZZ",       "Bearings",        "Ball Bearings",5.80,  14.50, "Each",  10, 150,  "true"),
    ("PRD008","V-Belt A42",            "Drives",          "Belts",        6.90,  16.00, "Each",  5,  80,   "true"),
    ("PRD009","Coupling Flex 25mm",    "Drives",          "Couplings",    18.40, 42.00, "Each",  12, 30,   "true"),
    ("PRD010","Conveyor Chain 40B",    "Conveyors",       "Chains",       35.00, 78.00, "Metre", 21, 20,   "true"),
    ("PRD011","Safety Valve 1/4in",    "Valves",          "Safety",       22.00, 55.00, "Each",  14, 40,   "true"),
    ("PRD012","Ball Valve 1/2in",      "Valves",          "Ball",         8.50,  21.00, "Each",  7,  60,   "true"),
    ("PRD013","Pressure Gauge 0-16bar","Instrumentation", "Gauges",       14.00, 32.00, "Each",  10, 25,   "true"),
    ("PRD014","Electric Motor 2.2kW",  "Electrical",      "Motors",       185.00,420.00,"Each",  30, 5,    "true"),
    ("PRD015","Control Panel 24VDC",   "Electrical",      "Panels",       320.00,750.00,"Each",  45, 3,    "false"),
], ["product_id","product_name","category","sub_category","unit_cost","unit_price",
    "uom","lead_time_days","reorder_point","is_active"])

write_csv_to_adls(products_df, "products")

# ── Purchase Orders ────────────────────────────────────────────────────────────
po_df = spark.createDataFrame([
    ("PO-2024-001","SUP001","2024-01-05","2024-01-19","2024-01-18","DELIVERED","USD",125.00,"Urgent restock"),
    ("PO-2024-002","SUP002","2024-01-08","2024-02-05","2024-02-06","DELIVERED","EUR",210.00,""),
    ("PO-2024-003","SUP003","2024-01-10","2024-03-10","2024-03-15","DELIVERED","USD",380.00,"Sea freight"),
    ("PO-2024-004","SUP004","2024-01-15","2024-01-29","2024-01-29","DELIVERED","CAD",95.00, ""),
    ("PO-2024-005","SUP005","2024-01-20","2024-02-17","2024-02-14","DELIVERED","CHF",175.00,"Early delivery"),
    ("PO-2024-006","SUP001","2024-02-01","2024-02-15","2024-02-16","DELIVERED","USD",140.00,""),
    ("PO-2024-007","SUP006","2024-02-05","2024-02-26","2024-03-01","DELIVERED","AUD",220.00,"Delayed customs"),
    ("PO-2024-008","SUP010","2024-02-10","2024-02-24","2024-02-24","DELIVERED","USD",88.00, ""),
    ("PO-2024-009","SUP002","2024-02-14","2024-03-13","2024-03-12","DELIVERED","EUR",195.00,""),
    ("PO-2024-010","SUP007","2024-02-20","2024-04-05",None,         "CANCELLED","SEK",0.00, "Supplier capacity issue"),
    ("PO-2024-011","SUP004","2024-03-01","2024-03-15","2024-03-14","DELIVERED","CAD",110.00,""),
    ("PO-2024-012","SUP001","2024-03-05","2024-03-19","2024-03-20","DELIVERED","USD",132.00,""),
    ("PO-2024-013","SUP003","2024-03-08","2024-05-08",None,         "CANCELLED","USD",0.00, "Quality dispute"),
    ("PO-2024-014","SUP010","2024-03-12","2024-03-26","2024-03-25","DELIVERED","USD",76.00, ""),
    ("PO-2024-015","SUP005","2024-03-15","2024-04-12","2024-04-11","DELIVERED","CHF",190.00,""),
    ("PO-2024-016","SUP008","2024-03-20","2024-04-03","2024-04-05","DELIVERED","MXN",850.00,""),
    ("PO-2024-017","SUP001","2024-04-02","2024-04-16",None,         "IN_TRANSIT","USD",145.00,""),
    ("PO-2024-018","SUP002","2024-04-05","2024-05-03",None,         "APPROVED", "EUR",230.00,""),
    ("PO-2024-019","SUP006","2024-04-08","2024-04-29",None,         "APPROVED", "AUD",205.00,""),
    ("PO-2024-020","SUP010","2024-04-10","2024-04-24",None,         "IN_TRANSIT","USD",92.00,""),
], ["po_id","supplier_id","po_date","expected_delivery_date","actual_delivery_date",
    "status","currency","shipping_cost","notes"])

write_csv_to_adls(po_df, "purchase_orders")

# ── Purchase Order Lines ───────────────────────────────────────────────────────
lines_df = spark.createDataFrame([
    ("LN-001","PO-2024-001","PRD001",1000,1000,0.15,"RECEIVED"),
    ("LN-002","PO-2024-001","PRD002",1000,1000,0.10,"RECEIVED"),
    ("LN-003","PO-2024-001","PRD003",2000,2000,0.05,"RECEIVED"),
    ("LN-004","PO-2024-002","PRD006",100, 100, 4.20,"RECEIVED"),
    ("LN-005","PO-2024-002","PRD007",80,  80,  5.80,"RECEIVED"),
    ("LN-006","PO-2024-002","PRD009",15,  15,  18.40,"RECEIVED"),
    ("LN-007","PO-2024-003","PRD001",5000,5000,0.14,"RECEIVED"),
    ("LN-008","PO-2024-003","PRD002",5000,4800,0.09,"PARTIAL"),
    ("LN-009","PO-2024-003","PRD003",8000,8000,0.05,"RECEIVED"),
    ("LN-010","PO-2024-004","PRD004",30,  30,  12.50,"RECEIVED"),
    ("LN-011","PO-2024-004","PRD005",60,  60,  3.75,"RECEIVED"),
    ("LN-012","PO-2024-005","PRD013",20,  20,  14.00,"RECEIVED"),
    ("LN-013","PO-2024-005","PRD011",25,  25,  22.00,"RECEIVED"),
    ("LN-014","PO-2024-005","PRD012",40,  40,  8.50,"RECEIVED"),
    ("LN-015","PO-2024-006","PRD001",2000,2000,0.15,"RECEIVED"),
    ("LN-016","PO-2024-006","PRD002",2000,2000,0.10,"RECEIVED"),
    ("LN-017","PO-2024-007","PRD008",50,  50,  6.90,"RECEIVED"),
    ("LN-018","PO-2024-007","PRD010",10,  8,   35.00,"PARTIAL"),
    ("LN-019","PO-2024-008","PRD001",3000,3000,0.15,"RECEIVED"),
    ("LN-020","PO-2024-008","PRD014",3,   3,   185.00,"RECEIVED"),
    ("LN-021","PO-2024-009","PRD006",150, 150, 4.20,"RECEIVED"),
    ("LN-022","PO-2024-009","PRD007",100, 100, 5.75,"RECEIVED"),
    ("LN-023","PO-2024-010","PRD010",20,  0,   35.00,"CANCELLED"),
    ("LN-024","PO-2024-011","PRD004",25,  25,  12.50,"RECEIVED"),
    ("LN-025","PO-2024-011","PRD005",50,  50,  3.75,"RECEIVED"),
    ("LN-026","PO-2024-012","PRD001",1500,1500,0.15,"RECEIVED"),
    ("LN-027","PO-2024-012","PRD002",1500,1500,0.10,"RECEIVED"),
    ("LN-028","PO-2024-012","PRD003",3000,3000,0.05,"RECEIVED"),
    ("LN-029","PO-2024-013","PRD001",4000,0,   0.14,"CANCELLED"),
    ("LN-030","PO-2024-013","PRD002",4000,0,   0.09,"CANCELLED"),
    ("LN-031","PO-2024-014","PRD014",2,   2,   185.00,"RECEIVED"),
    ("LN-032","PO-2024-014","PRD013",15,  15,  14.00,"RECEIVED"),
    ("LN-033","PO-2024-015","PRD011",30,  30,  22.00,"RECEIVED"),
    ("LN-034","PO-2024-015","PRD012",60,  60,  8.50,"RECEIVED"),
    ("LN-035","PO-2024-015","PRD013",10,  10,  14.00,"RECEIVED"),
    ("LN-036","PO-2024-016","PRD001",3000,3000,0.13,"RECEIVED"),
    ("LN-037","PO-2024-016","PRD008",40,  40,  6.80,"RECEIVED"),
    ("LN-038","PO-2024-017","PRD001",2500,0,   0.15,"PENDING"),
    ("LN-039","PO-2024-017","PRD002",2500,0,   0.10,"PENDING"),
    ("LN-040","PO-2024-017","PRD003",5000,0,   0.05,"PENDING"),
    ("LN-041","PO-2024-018","PRD006",200, 0,   4.15,"PENDING"),
    ("LN-042","PO-2024-018","PRD007",150, 0,   5.70,"PENDING"),
    ("LN-043","PO-2024-019","PRD008",60,  0,   6.90,"PENDING"),
    ("LN-044","PO-2024-019","PRD009",20,  0,   18.40,"PENDING"),
    ("LN-045","PO-2024-020","PRD014",4,   0,   185.00,"PENDING"),
    ("LN-046","PO-2024-020","PRD013",25,  0,   14.00,"PENDING"),
], ["line_id","po_id","product_id","quantity_ordered","quantity_received","unit_cost","line_status"])

write_csv_to_adls(lines_df, "purchase_order_lines")

# COMMAND ----------
# MAGIC %md ## 8. Verify Landing Zone on ADLS

# COMMAND ----------

print(f"Landing zone contents on ADLS ({LANDING_PATH}/):")
for folder in dbutils.fs.ls(f"{LANDING_PATH}/"):
    files = dbutils.fs.ls(folder.path)
    csv_files = [f for f in files if f.name.endswith(".csv")]
    print(f"  {folder.name}  ({len(csv_files)} CSV file(s))")

# COMMAND ----------

print("\n" + "="*60)
print("SETUP COMPLETE")
print("="*60)
print(f"  Storage Credential : {CREDENTIAL_NAME}")
print(f"  External Locations : {EXT_LOC_LANDING}, {EXT_LOC_CATALOG}")
print(f"  Catalog            : {CATALOG}  (MANAGED LOCATION → ADLS)")
print(f"  Landing Path       : {LANDING_PATH}")
print(f"  Catalog Path       : {CATALOG_PATH}")
print(f"  Schemas            : bronze | silver | gold | audit")
print(f"  Audit Tables       : pipeline_run_log | dq_check_results")
print("="*60)
print("\nNext: Run nb_01_bronze_ingest.py")
