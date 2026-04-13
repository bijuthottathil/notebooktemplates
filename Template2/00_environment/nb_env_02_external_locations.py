# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_02_external_locations
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Register ADLS Gen2 container paths as Unity Catalog External Locations
# MAGIC **Run Order:** Must run after nb_env_01_storage_credentials

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

ENV             = "dev"                  # dev | staging | prod
STORAGE_ACCOUNT = "dlscompanydev"        # ADLS Gen2 storage account name
CONTAINER       = "datalake"             # ADLS container name
CREDENTIAL_NAME = "adls_sp_credential"   # Must match nb_env_01 credential name

# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import Row
import datetime

BASE_URL = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

EXTERNAL_LOCATIONS = {
    f"ext_loc_landing_{ENV}": f"{BASE_URL}/landing/",
    f"ext_loc_bronze_{ENV}":  f"{BASE_URL}/bronze/",
    f"ext_loc_silver_{ENV}":  f"{BASE_URL}/silver/",
    f"ext_loc_gold_{ENV}":    f"{BASE_URL}/gold/",
}

print(f"Storage Account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print(f"Base URL        : {BASE_URL}")

# COMMAND ----------
# MAGIC %md ## 1. Create External Locations

# COMMAND ----------

for location_name, url in EXTERNAL_LOCATIONS.items():
    try:
        spark.sql(f"""
            CREATE EXTERNAL LOCATION IF NOT EXISTS `{location_name}`
            URL '{url}'
            WITH (STORAGE CREDENTIAL `{CREDENTIAL_NAME}`)
            COMMENT 'Managed by nb_env_02 – {ENV} – auto-provisioned'
        """)
        print(f"  [OK] {location_name} → {url}")
    except Exception as e:
        print(f"  [WARN] {location_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 2. Validate External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------
# MAGIC %md ## 3. Test Read/Write Access

# COMMAND ----------

def test_location_access(location_name, url):
    probe_path = f"{url}_access_probe/probe_{datetime.date.today()}.json"
    result = {"location": location_name, "status": "UNKNOWN", "error": None}
    try:
        probe_df = spark.createDataFrame([Row(probe="ok", ts=str(datetime.datetime.utcnow()))])
        probe_df.write.mode("overwrite").json(probe_path)
        spark.read.json(probe_path).count()
        dbutils.fs.rm(probe_path, recurse=True)
        result["status"] = "PASS"
    except Exception as e:
        result["status"] = "FAIL"
        result["error"]  = str(e)
    return result

results = [test_location_access(name, url) for name, url in EXTERNAL_LOCATIONS.items()]
for r in results:
    icon = "✓" if r["status"] == "PASS" else "✗"
    print(f"  [{icon}] {r['status']:4s} | {r['location']}")
    if r["error"]:
        print(f"         Error: {r['error']}")

failed = [r for r in results if r["status"] == "FAIL"]
if failed:
    raise RuntimeError(f"{len(failed)} external location(s) failed access test.")

print("\nNext Step: Run nb_env_03_catalog_schema_setup")
