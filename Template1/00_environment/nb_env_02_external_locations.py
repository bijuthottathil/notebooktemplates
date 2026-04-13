# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_02_external_locations
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Register ADLS Gen2 container paths as Unity Catalog External Locations
# MAGIC **Run Order:** Must run after `nb_env_01_storage_credentials`
# MAGIC
# MAGIC **ADLS Path Convention:**
# MAGIC ```
# MAGIC abfss://<container>@<storage_account>.dfs.core.windows.net/<layer>/
# MAGIC   ├── landing/     ← raw file drops (CSV, Excel, JSON, Parquet)
# MAGIC   ├── bronze/      ← Delta tables, raw ingested
# MAGIC   ├── silver/      ← Delta tables, cleansed/conformed
# MAGIC   └── gold/        ← Delta tables, aggregated marts
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("storage_account", "dlscompanydev", "ADLS Storage Account Name")
dbutils.widgets.text("container", "datalake", "ADLS Container Name")
dbutils.widgets.text("credential_name", "adls_sp_credential", "Storage Credential Name")

ENV              = dbutils.widgets.get("env")
STORAGE_ACCOUNT  = dbutils.widgets.get("storage_account")
CONTAINER        = dbutils.widgets.get("container")
CREDENTIAL_NAME  = dbutils.widgets.get("credential_name")

BASE_URL = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# External location definitions: name → ADLS path suffix
EXTERNAL_LOCATIONS = {
    f"ext_loc_landing_{ENV}": f"{BASE_URL}/landing/",
    f"ext_loc_bronze_{ENV}":  f"{BASE_URL}/bronze/",
    f"ext_loc_silver_{ENV}":  f"{BASE_URL}/silver/",
    f"ext_loc_gold_{ENV}":    f"{BASE_URL}/gold/",
}

print(f"Environment    : {ENV}")
print(f"Storage Account: {STORAGE_ACCOUNT}")
print(f"Container      : {CONTAINER}")
print(f"Base URL       : {BASE_URL}")

# COMMAND ----------
# MAGIC %md ## 2. Create External Locations

# COMMAND ----------

for location_name, url in EXTERNAL_LOCATIONS.items():
    ddl = f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `{location_name}`
    URL '{url}'
    WITH (STORAGE CREDENTIAL `{CREDENTIAL_NAME}`)
    COMMENT 'Managed by nb_env_02 – {ENV} – auto-provisioned'
    """
    try:
        spark.sql(ddl)
        print(f"  [OK] {location_name} → {url}")
    except Exception as e:
        print(f"  [WARN] {location_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 3. Validate External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------
# MAGIC %md ## 4. Test Read/Write Access per Location

# COMMAND ----------

from pyspark.sql import Row
import datetime

def test_location_access(location_name: str, url: str) -> dict:
    """Write a probe file and read it back to validate RW access."""
    probe_path = f"{url}_access_probe/probe_{datetime.date.today()}.json"
    result = {"location": location_name, "url": url, "status": "UNKNOWN", "error": None}
    try:
        probe_df = spark.createDataFrame([Row(probe="ok", ts=str(datetime.datetime.utcnow()))])
        probe_df.write.mode("overwrite").json(probe_path)
        spark.read.json(probe_path).count()
        dbutils.fs.rm(probe_path, recurse=True)
        result["status"] = "PASS"
    except Exception as e:
        result["status"] = "FAIL"
        result["error"] = str(e)
    return result

results = [test_location_access(name, url) for name, url in EXTERNAL_LOCATIONS.items()]

for r in results:
    icon = "✓" if r["status"] == "PASS" else "✗"
    print(f"  [{icon}] {r['status']:4s} | {r['location']}")
    if r["error"]:
        print(f"         Error: {r['error']}")

failed = [r for r in results if r["status"] == "FAIL"]
if failed:
    raise RuntimeError(f"{len(failed)} external location(s) failed access test. Review errors above.")

# COMMAND ----------
# MAGIC %md ## 5. Output Paths for Downstream Notebooks

# COMMAND ----------

# Expose as task values for downstream jobs in a Databricks Workflow
for location_name, url in EXTERNAL_LOCATIONS.items():
    key = location_name.replace(f"_{ENV}", "").replace("ext_loc_", "path_")
    dbutils.jobs.taskValues.set(key=key, value=url)
    print(f"  Task value set: {key} = {url}")

print("\nNext Step: Run nb_env_03_catalog_schema_setup")
