# Databricks notebook source
# MAGIC %md
# MAGIC # nb_00_setup_and_load_data
# MAGIC **Purpose:** One-time infrastructure setup for the health insurance ETL pipeline.
# MAGIC Creates storage credentials, external locations, Unity Catalog, schemas, audit tables,
# MAGIC and uploads sample health insurance CSV files to the ADLS landing zone.
# MAGIC
# MAGIC **Credential configuration is loaded from `.env`** (replaces `dbutils.widgets`).
# MAGIC Edit `.env` in this folder before running.
# MAGIC
# MAGIC **Setup sequence:**
# MAGIC ```
# MAGIC 1. Load .env                (credential names and secret scope/key config)
# MAGIC 2. Storage Credential       (service principal → Unity Catalog)
# MAGIC 3. External Locations       (ADLS paths → Unity Catalog)
# MAGIC 4. Validate Access          (probe read/write on each external location)
# MAGIC 5. Catalog                  (with MANAGED LOCATION on ADLS)
# MAGIC 6. Schemas                  (bronze / silver / gold / audit)
# MAGIC 7. Audit Tables             (pipeline_run_log, dq_check_results)
# MAGIC 8. Generate & Upload CSVs   (plans, members, providers, claims → ADLS landing)
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Install & Load `.env`
# MAGIC
# MAGIC `python-dotenv` reads the `.env` file from the same directory as this notebook
# MAGIC in the Databricks Repo workspace. It replaces the five `dbutils.widgets.text()`
# MAGIC calls that would otherwise be needed for credential configuration.

# COMMAND ----------

# MAGIC %pip install python-dotenv -q

# COMMAND ----------

import os
from dotenv import load_dotenv

# Resolve the directory this notebook lives in (works in Databricks Repos)
_nb_ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = _nb_ctx.notebookPath().get()                       # e.g. /Repos/user/repo/etl-template1/nb_00...
_nb_dir  = "/Workspace" + "/".join(_nb_path.split("/")[:-1])  # strip notebook name → folder path

_env_file = os.path.join(_nb_dir, ".env")
load_dotenv(_env_file)
print(f"Loaded .env from: {_env_file}")

# Credential config — sourced from .env (replaces dbutils.widgets)
CREDENTIAL_NAME       = os.getenv("CREDENTIAL_NAME",       "adls_sp_credential")
CLIENT_ID_SECRET_SCOPE= os.getenv("CLIENT_ID_SECRET_SCOPE","kv-databricks")
CLIENT_ID_SECRET_KEY  = os.getenv("CLIENT_ID_SECRET_KEY",  "sp-client-id")
CLIENT_SECRET_KEY     = os.getenv("CLIENT_SECRET_KEY",     "sp-client-secret")
TENANT_ID_SECRET_KEY  = os.getenv("TENANT_ID_SECRET_KEY",  "sp-tenant-id")

print(f"CREDENTIAL_NAME        : {CREDENTIAL_NAME}")
print(f"CLIENT_ID_SECRET_SCOPE : {CLIENT_ID_SECRET_SCOPE}")
print(f"CLIENT_ID_SECRET_KEY   : {CLIENT_ID_SECRET_KEY}")
print(f"CLIENT_SECRET_KEY      : {CLIENT_SECRET_KEY}")
print(f"TENANT_ID_SECRET_KEY   : {TENANT_ID_SECRET_KEY}")

# COMMAND ----------
# MAGIC %md ## Configuration
# MAGIC Edit this section to match your Azure environment.

# COMMAND ----------

# ── ADLS Gen2 ─────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = "dlshealthdev"
CONTAINER       = "datalake"
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/etl_template1"

# ── Unity Catalog ─────────────────────────────────────────────────────────────
CATALOG         = "health_insurance_dev"

# ── Derived paths ─────────────────────────────────────────────────────────────
LANDING_PATH    = f"{ADLS_BASE}/landing"
CATALOG_PATH    = f"{ADLS_BASE}/catalog"

EXT_LOC_LANDING = "ext_loc_hi_landing"
EXT_LOC_CATALOG = "ext_loc_hi_catalog"

# ─────────────────────────────────────────────────────────────────────────────

print(f"Storage Account : {STORAGE_ACCOUNT}")
print(f"ADLS Base       : {ADLS_BASE}")
print(f"Landing Path    : {LANDING_PATH}")
print(f"Catalog Path    : {CATALOG_PATH}")
print(f"Catalog         : {CATALOG}")

# COMMAND ----------
# MAGIC %md ## 2. Create Storage Credential

# COMMAND ----------

client_id     = dbutils.secrets.get(scope=CLIENT_ID_SECRET_SCOPE, key=CLIENT_ID_SECRET_KEY)
client_secret = dbutils.secrets.get(scope=CLIENT_ID_SECRET_SCOPE, key=CLIENT_SECRET_KEY)
tenant_id     = dbutils.secrets.get(scope=CLIENT_ID_SECRET_SCOPE, key=TENANT_ID_SECRET_KEY)

print("Secrets retrieved from Key Vault scope.")

spark.sql(f"""
    CREATE STORAGE CREDENTIAL IF NOT EXISTS `{CREDENTIAL_NAME}`
    WITH AZURE_SERVICE_CREDENTIAL (
        DIRECTORY_ID   = '{tenant_id}',
        APPLICATION_ID = '{client_id}',
        CLIENT_SECRET  = '{client_secret}'
    )
    COMMENT 'Service principal credential for ADLS Gen2 – etl_template1 health insurance demo'
""")

print(f"Storage credential ready: {CREDENTIAL_NAME}")
spark.sql(f"DESCRIBE STORAGE CREDENTIAL `{CREDENTIAL_NAME}`").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 3. Create External Locations
# MAGIC
# MAGIC | External Location    | ADLS Path                           | Used for                     |
# MAGIC |----------------------|-------------------------------------|------------------------------|
# MAGIC | ext_loc_hi_landing   | `.../etl_template1/landing/`        | CSV file drops (read only)   |
# MAGIC | ext_loc_hi_catalog   | `.../etl_template1/catalog/`        | Catalog managed Delta tables |

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
            COMMENT 'etl_template1 health insurance demo – managed by nb_00'
        """)
        print(f"  [OK] {loc_name} → {url}")
    except Exception as e:
        print(f"  [WARN] {loc_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 4. Validate External Location Access

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
    raise RuntimeError("One or more external locations failed access validation.")

print("\nAll external locations validated.")

# COMMAND ----------
# MAGIC %md ## 5. Create Unity Catalog with Managed Location

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG}`
    MANAGED LOCATION '{CATALOG_PATH}/'
    COMMENT 'Health insurance ETL demo – managed tables stored on ADLS via ext_loc_hi_catalog'
""")
spark.sql(f"USE CATALOG `{CATALOG}`")

print(f"Catalog ready: {CATALOG}")
print(f"  Managed location: {CATALOG_PATH}/")

# COMMAND ----------
# MAGIC %md ## 6. Create Schemas

# COMMAND ----------

for schema, comment in {
    "bronze": "Raw ingested CSV data — no transformations",
    "silver": "Cleansed and conformed health insurance data",
    "gold":   "Business aggregations, KPI marts, and reports",
    "audit":  "Pipeline run logs and DQ check results",
}.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`
        COMMENT '{comment}'
    """)
    print(f"  Schema ready: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md ## 7. Create Audit Tables

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
# MAGIC %md ## 8. Generate Sample Data & Upload to ADLS Landing Zone
# MAGIC
# MAGIC Four health insurance CSV datasets are generated and written directly to ADLS.
# MAGIC
# MAGIC | Entity    | Rows | Description |
# MAGIC |-----------|------|-------------|
# MAGIC | plans     | 4    | Insurance plan definitions (PPO/HMO/EPO, metal tiers) |
# MAGIC | members   | 15   | Enrolled members across plans and states |
# MAGIC | providers | 8    | Physicians and facilities (in/out of network) |
# MAGIC | claims    | 52   | Medical, pharmacy, and dental claims |

# COMMAND ----------

from pyspark.sql import functions as F

def write_csv_to_adls(df, entity_name):
    """Write a DataFrame as a single CSV file to the ADLS landing zone."""
    path = f"{LANDING_PATH}/{entity_name}/"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  [OK] {entity_name} → {path}  ({df.count()} rows)")

# ── Plans ──────────────────────────────────────────────────────────────────────
plans_df = spark.createDataFrame([
    ("PL001", "Gold PPO",      "PPO", "Gold",     1000.00, 5000.00,  650.00, "2024-01-01", ""),
    ("PL002", "Silver HMO",    "HMO", "Silver",   2500.00, 7500.00,  450.00, "2024-01-01", ""),
    ("PL003", "Bronze EPO",    "EPO", "Bronze",   5000.00, 8500.00,  280.00, "2024-01-01", ""),
    ("PL004", "Platinum PPO",  "PPO", "Platinum",  500.00, 3000.00,  850.00, "2024-01-01", ""),
], ["plan_id", "plan_name", "plan_type", "metal_tier",
    "annual_deductible", "oop_max", "premium_monthly", "effective_date", "termination_date"])

write_csv_to_adls(plans_df, "plans")

# ── Members ────────────────────────────────────────────────────────────────────
members_df = spark.createDataFrame([
    ("MBR001", "PL001", "James",   "Carter",   "1978-03-14", "M", "TX", "75201", "2024-01-01", "",           "true"),
    ("MBR002", "PL002", "Sofia",   "Martinez", "1985-07-22", "F", "FL", "33101", "2024-01-01", "",           "true"),
    ("MBR003", "PL004", "David",   "Kim",      "1970-11-05", "M", "CA", "90001", "2024-01-01", "",           "true"),
    ("MBR004", "PL003", "Emily",   "Thompson", "1992-05-30", "F", "NY", "10001", "2024-01-01", "",           "true"),
    ("MBR005", "PL001", "Marcus",  "Johnson",  "1965-09-18", "M", "IL", "60601", "2024-01-01", "",           "true"),
    ("MBR006", "PL002", "Linda",   "Patel",    "1989-02-27", "F", "GA", "30301", "2024-01-01", "",           "true"),
    ("MBR007", "PL004", "Robert",  "Williams", "1955-12-03", "M", "OH", "44101", "2024-01-01", "",           "true"),
    ("MBR008", "PL003", "Aisha",   "Brown",    "1997-06-15", "F", "NC", "27601", "2024-01-01", "",           "true"),
    ("MBR009", "PL001", "Carlos",  "Gomez",    "1983-08-09", "M", "AZ", "85001", "2024-02-01", "",           "true"),
    ("MBR010", "PL002", "Rachel",  "Davis",    "1974-04-21", "F", "WA", "98101", "2024-02-01", "",           "true"),
    ("MBR011", "PL004", "Thomas",  "Wilson",   "1961-10-12", "M", "MA", "02101", "2024-02-01", "",           "true"),
    ("MBR012", "PL003", "Priya",   "Sharma",   "1990-01-28", "F", "TX", "77001", "2024-02-01", "",           "true"),
    ("MBR013", "PL001", "Kevin",   "Lee",      "1987-07-04", "M", "CA", "94101", "2024-03-01", "",           "true"),
    ("MBR014", "PL002", "Nancy",   "Robinson", "1968-03-17", "F", "PA", "19101", "2024-03-01", "2024-09-30", "false"),
    ("MBR015", "PL003", "Derek",   "Clark",    "1995-11-23", "M", "CO", "80201", "2024-03-01", "",           "true"),
], ["member_id", "plan_id", "first_name", "last_name", "dob", "gender",
    "state", "zip_code", "enrollment_date", "termination_date", "is_active"])

write_csv_to_adls(members_df, "members")

# ── Providers ──────────────────────────────────────────────────────────────────
providers_df = spark.createDataFrame([
    ("PRV001", "Dr. Anne Harrison",       "PCP",        "Family Medicine",       "TX", "1234567890", "Tier 1", "true"),
    ("PRV002", "Dr. Samuel Ortega",       "Specialist", "Cardiology",            "FL", "2345678901", "Tier 1", "true"),
    ("PRV003", "Sunrise Medical Center",  "Hospital",   "General",               "CA", "3456789012", "Tier 1", "true"),
    ("PRV004", "Dr. Mei Lin",             "Specialist", "Endocrinology",         "NY", "4567890123", "Tier 2", "true"),
    ("PRV005", "Green Valley Urgent Care","Urgent Care","Urgent Care",           "IL", "5678901234", "Tier 1", "true"),
    ("PRV006", "Dr. Brian Foster",        "PCP",        "Internal Medicine",     "GA", "6789012345", "Tier 2", "true"),
    ("PRV007", "Pinnacle Orthopedics",    "Specialist", "Orthopedics",           "OH", "7890123456", "Tier 1", "true"),
    ("PRV008", "ClearView Pharmacy",      "Pharmacy",   "Retail Pharmacy",       "TX", "8901234567", "Tier 1", "true"),
], ["provider_id", "provider_name", "provider_type", "specialty",
    "state", "npi_number", "network_tier", "is_in_network"])

write_csv_to_adls(providers_df, "providers")

# ── Claims ─────────────────────────────────────────────────────────────────────
claims_df = spark.createDataFrame([
    # Medical claims — Q1
    ("CLM001","MBR001","PRV001","PL001","2024-01-10","2024-01-10","MEDICAL","Z00.00","99213", 250.00, 180.00, 140.00, 40.00,   0.00,  "PAID"),
    ("CLM002","MBR002","PRV002","PL002","2024-01-12","2024-01-11","MEDICAL","I10",   "93000", 850.00, 600.00, 350.00, 80.00,  170.00, "PAID"),
    ("CLM003","MBR003","PRV003","PL004","2024-01-15","2024-01-14","MEDICAL","M54.5", "99214", 380.00, 290.00, 265.00, 25.00,   0.00,  "PAID"),
    ("CLM004","MBR004","PRV004","PL003","2024-01-18","2024-01-17","MEDICAL","E11.9", "99215", 420.00, 300.00, 175.00, 50.00,  75.00,  "PAID"),
    ("CLM005","MBR005","PRV001","PL001","2024-01-22","2024-01-22","MEDICAL","J06.9", "99213", 200.00, 150.00, 110.00, 40.00,   0.00,  "PAID"),
    ("CLM006","MBR006","PRV006","PL002","2024-01-25","2024-01-24","MEDICAL","Z00.00","99213", 240.00, 175.00, 100.00, 30.00,  45.00,  "PAID"),
    ("CLM007","MBR007","PRV002","PL004","2024-01-28","2024-01-27","MEDICAL","I25.10","93306",3200.00,2400.00,2275.00,125.00,  0.00,  "PAID"),
    ("CLM008","MBR008","PRV005","PL003","2024-02-02","2024-02-02","MEDICAL","S93.40","99283", 650.00, 420.00, 220.00, 75.00, 125.00,  "PAID"),
    ("CLM009","MBR009","PRV001","PL001","2024-02-05","2024-02-05","MEDICAL","Z00.00","99213", 250.00, 180.00, 140.00, 40.00,   0.00,  "PAID"),
    ("CLM010","MBR010","PRV006","PL002","2024-02-08","2024-02-07","MEDICAL","N39.0", "99214", 310.00, 230.00, 130.00, 30.00,  70.00,  "PAID"),
    ("CLM011","MBR011","PRV003","PL004","2024-02-12","2024-02-11","MEDICAL","K21.0", "43239",4500.00,3200.00,3075.00,125.00,  0.00,  "PAID"),
    ("CLM012","MBR012","PRV004","PL003","2024-02-15","2024-02-14","MEDICAL","E11.65","99214", 400.00, 290.00, 165.00, 50.00,  75.00,  "PAID"),
    ("CLM013","MBR013","PRV001","PL001","2024-02-20","2024-02-20","MEDICAL","J06.9", "99213", 200.00, 150.00, 110.00, 40.00,   0.00,  "PAID"),
    ("CLM014","MBR014","PRV006","PL002","2024-02-22","2024-02-21","MEDICAL","Z00.00","99213", 240.00, 175.00,  95.00, 30.00,  50.00,  "PAID"),
    ("CLM015","MBR015","PRV005","PL003","2024-02-26","2024-02-26","MEDICAL","M25.511","99283",580.00, 390.00, 200.00, 65.00, 125.00,  "PAID"),
    # Pharmacy claims — Q1
    ("CLM016","MBR001","PRV008","PL001","2024-01-15","2024-01-15","PHARMACY","","",   120.00,  90.00,  75.00, 15.00,   0.00,  "PAID"),
    ("CLM017","MBR002","PRV008","PL002","2024-01-20","2024-01-20","PHARMACY","","",   280.00, 200.00, 130.00, 40.00,  30.00,  "PAID"),
    ("CLM018","MBR003","PRV008","PL004","2024-01-25","2024-01-25","PHARMACY","","",    85.00,  70.00,  65.00,  5.00,   0.00,  "PAID"),
    ("CLM019","MBR007","PRV008","PL004","2024-02-01","2024-02-01","PHARMACY","","",   350.00, 260.00, 245.00, 15.00,   0.00,  "PAID"),
    ("CLM020","MBR005","PRV008","PL001","2024-02-10","2024-02-10","PHARMACY","","",   150.00, 110.00,  80.00, 15.00,  15.00,  "PAID"),
    ("CLM021","MBR012","PRV008","PL003","2024-02-18","2024-02-18","PHARMACY","","",   420.00, 310.00, 200.00, 40.00,  70.00,  "PAID"),
    # Medical claims — Q2
    ("CLM022","MBR001","PRV001","PL001","2024-04-03","2024-04-03","MEDICAL","Z00.00","99213", 250.00, 180.00, 140.00, 40.00,   0.00,  "PAID"),
    ("CLM023","MBR003","PRV007","PL004","2024-04-08","2024-04-07","MEDICAL","M17.11","27447",18500.0,14000.0,13875.0,125.00,  0.00,  "PAID"),
    ("CLM024","MBR005","PRV002","PL001","2024-04-10","2024-04-09","MEDICAL","I10",   "93000", 850.00, 600.00, 530.00, 40.00,  30.00,  "PAID"),
    ("CLM025","MBR009","PRV001","PL001","2024-04-12","2024-04-12","MEDICAL","J06.9", "99213", 200.00, 150.00, 110.00, 40.00,   0.00,  "PAID"),
    ("CLM026","MBR011","PRV003","PL004","2024-04-15","2024-04-14","MEDICAL","Z00.00","99214", 320.00, 240.00, 215.00, 25.00,   0.00,  "PAID"),
    ("CLM027","MBR006","PRV004","PL002","2024-04-18","2024-04-17","MEDICAL","E11.9", "99215", 420.00, 300.00, 170.00, 50.00,  80.00,  "PAID"),
    ("CLM028","MBR008","PRV007","PL003","2024-04-20","2024-04-19","MEDICAL","M54.5", "27096",2800.00,2000.00,1500.00,100.00, 400.00, "PAID"),
    ("CLM029","MBR010","PRV006","PL002","2024-04-22","2024-04-22","MEDICAL","Z00.00","99213", 240.00, 175.00,  95.00, 30.00,  50.00,  "PAID"),
    ("CLM030","MBR013","PRV001","PL001","2024-04-25","2024-04-25","MEDICAL","Z00.00","99213", 250.00, 180.00, 140.00, 40.00,   0.00,  "PAID"),
    ("CLM031","MBR015","PRV005","PL003","2024-04-28","2024-04-28","MEDICAL","J06.9", "99213", 195.00, 145.00,  50.00, 30.00,  65.00,  "PAID"),
    # Pharmacy claims — Q2
    ("CLM032","MBR002","PRV008","PL002","2024-04-05","2024-04-05","PHARMACY","","",   280.00, 200.00, 130.00, 40.00,  30.00,  "PAID"),
    ("CLM033","MBR007","PRV008","PL004","2024-04-12","2024-04-12","PHARMACY","","",   350.00, 260.00, 245.00, 15.00,   0.00,  "PAID"),
    ("CLM034","MBR011","PRV008","PL004","2024-04-19","2024-04-19","PHARMACY","","",   220.00, 165.00, 150.00, 15.00,   0.00,  "PAID"),
    # Dental claims
    ("CLM035","MBR001","PRV001","PL001","2024-03-05","2024-03-05","DENTAL","Z01.20","D0120",  95.00,  80.00,  64.00, 16.00,   0.00,  "PAID"),
    ("CLM036","MBR004","PRV001","PL003","2024-03-10","2024-03-10","DENTAL","Z01.20","D0150", 125.00, 100.00,  40.00, 20.00,  40.00,  "PAID"),
    ("CLM037","MBR009","PRV001","PL001","2024-03-15","2024-03-15","DENTAL","Z01.20","D0120",  95.00,  80.00,  64.00, 16.00,   0.00,  "PAID"),
    ("CLM038","MBR013","PRV001","PL001","2024-05-08","2024-05-08","DENTAL","K02.9", "D2391", 320.00, 260.00, 208.00, 52.00,   0.00,  "PAID"),
    # Pending / denied claims
    ("CLM039","MBR002","PRV002","PL002","2024-05-02","2024-05-01","MEDICAL","I10",   "93306",3500.00,2500.00,   0.00,  0.00,   0.00,  "PENDING"),
    ("CLM040","MBR012","PRV004","PL003","2024-05-05","2024-05-04","MEDICAL","E11.9", "99215", 420.00, 300.00,  0.00,   0.00,   0.00,  "PENDING"),
    ("CLM041","MBR008","PRV007","PL003","2024-05-10","2024-05-09","MEDICAL","M17.11","27447",18500.0,14000.0,  0.00,   0.00,   0.00,  "PENDING"),
    ("CLM042","MBR015","PRV005","PL003","2024-05-12","2024-05-12","MEDICAL","J45.41","99213", 195.00, 145.00,   0.00,  0.00,   0.00,  "DENIED"),
    # Q3 Medical (partial — batch in progress)
    ("CLM043","MBR001","PRV001","PL001","2024-07-08","2024-07-08","MEDICAL","Z00.00","99213", 260.00, 188.00, 148.00, 40.00,   0.00,  "PAID"),
    ("CLM044","MBR003","PRV003","PL004","2024-07-10","2024-07-09","MEDICAL","K21.0", "43239",4600.00,3280.00,3155.00,125.00,  0.00,  "PAID"),
    ("CLM045","MBR005","PRV001","PL001","2024-07-15","2024-07-15","MEDICAL","J06.9", "99213", 210.00, 158.00, 118.00, 40.00,   0.00,  "PAID"),
    ("CLM046","MBR007","PRV002","PL004","2024-07-18","2024-07-17","MEDICAL","I25.10","93306",3300.00,2480.00,2355.00,125.00,  0.00,  "PAID"),
    ("CLM047","MBR010","PRV006","PL002","2024-07-22","2024-07-21","MEDICAL","N39.0", "99214", 320.00, 238.00, 138.00, 30.00,  70.00,  "PAID"),
    ("CLM048","MBR011","PRV003","PL004","2024-07-25","2024-07-24","MEDICAL","Z00.00","99214", 330.00, 248.00, 223.00, 25.00,   0.00,  "PAID"),
    # Q3 Pharmacy
    ("CLM049","MBR001","PRV008","PL001","2024-07-12","2024-07-12","PHARMACY","","",  125.00,  93.00,  78.00, 15.00,   0.00,  "PAID"),
    ("CLM050","MBR007","PRV008","PL004","2024-07-20","2024-07-20","PHARMACY","","",  360.00, 268.00, 253.00, 15.00,   0.00,  "PAID"),
    ("CLM051","MBR011","PRV008","PL004","2024-07-28","2024-07-28","PHARMACY","","",  225.00, 170.00, 155.00, 15.00,   0.00,  "PAID"),
    ("CLM052","MBR012","PRV008","PL003","2024-07-30","2024-07-30","PHARMACY","","",  430.00, 318.00, 208.00, 40.00,  70.00,  "PAID"),
], ["claim_id","member_id","provider_id","plan_id","claim_date","service_date",
    "claim_type","diagnosis_code","procedure_code",
    "billed_amount","allowed_amount","paid_amount",
    "member_copay","member_deductible","claim_status"])

write_csv_to_adls(claims_df, "claims")

# COMMAND ----------
# MAGIC %md ## Verify Landing Zone

# COMMAND ----------

print(f"Landing zone contents on ADLS ({LANDING_PATH}/):")
for folder in dbutils.fs.ls(f"{LANDING_PATH}/"):
    files    = dbutils.fs.ls(folder.path)
    csv_files = [f for f in files if f.name.endswith(".csv")]
    print(f"  {folder.name}  ({len(csv_files)} CSV file(s))")

# COMMAND ----------

print("\n" + "="*60)
print("SETUP COMPLETE")
print("="*60)
print(f"  Storage Credential : {CREDENTIAL_NAME}")
print(f"  External Locations : {EXT_LOC_LANDING}, {EXT_LOC_CATALOG}")
print(f"  Catalog            : {CATALOG}  (MANAGED LOCATION → ADLS)")
print(f"  Schemas            : bronze | silver | gold | audit")
print(f"  Audit Tables       : pipeline_run_log | dq_check_results")
print(f"  Landing Path       : {LANDING_PATH}")
print("="*60)
print("\nNext: Run nb_01_bronze_ingest.py")
