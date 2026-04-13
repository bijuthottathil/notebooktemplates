# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_01_storage_credentials
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Register Azure service principal credentials in Unity Catalog for ADLS Gen2 access
# MAGIC **Naming Convention:** `nb_<layer>_<seq>_<purpose>`
# MAGIC **Run Order:** 01 → 02 → 03 (environment notebooks must run in sequence)
# MAGIC
# MAGIC | Attribute       | Value                          |
# MAGIC |-----------------|-------------------------------|
# MAGIC | Author          | Data Platform Team             |
# MAGIC | Layer           | Environment / Infrastructure   |
# MAGIC | Frequency       | One-time setup (re-run on rotation) |
# MAGIC | Requires        | Metastore admin or credential admin privilege |

# COMMAND ----------
# MAGIC %md ## 1. Parameters

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("credential_name", "adls_sp_credential", "Credential Name")
dbutils.widgets.text("client_id_secret_scope", "kv-databricks", "Secret Scope (Key Vault)")
dbutils.widgets.text("client_id_secret_key", "sp-client-id", "Secret Key: Client ID")
dbutils.widgets.text("client_secret_key", "sp-client-secret", "Secret Key: Client Secret")
dbutils.widgets.text("tenant_id_secret_key", "sp-tenant-id", "Secret Key: Tenant ID")

ENV             = dbutils.widgets.get("env")
CREDENTIAL_NAME = dbutils.widgets.get("credential_name")
SECRET_SCOPE    = dbutils.widgets.get("client_id_secret_scope")
CLIENT_ID_KEY   = dbutils.widgets.get("client_id_secret_key")
CLIENT_SECRET_KEY = dbutils.widgets.get("client_secret_key")
TENANT_ID_KEY   = dbutils.widgets.get("tenant_id_secret_key")

print(f"Environment     : {ENV}")
print(f"Credential Name : {CREDENTIAL_NAME}")
print(f"Secret Scope    : {SECRET_SCOPE}")

# COMMAND ----------
# MAGIC %md ## 2. Retrieve Secrets from Azure Key Vault-backed Scope

# COMMAND ----------

client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_ID_KEY)
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_SECRET_KEY)
tenant_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=TENANT_ID_KEY)

print("Secrets retrieved successfully.")

# COMMAND ----------
# MAGIC %md ## 3. Create Storage Credential (Unity Catalog)
# MAGIC
# MAGIC Creates a **service credential** using a service principal. This credential is referenced by
# MAGIC External Locations (`nb_env_02`) to grant Databricks access to ADLS Gen2 containers.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop if exists for idempotent re-runs (safe in dev/staging; guard in prod)
# MAGIC -- REVOKE ALL ON STORAGE CREDENTIAL ${credential_name} FROM `account users`;

# COMMAND ----------

create_credential_sql = f"""
CREATE STORAGE CREDENTIAL IF NOT EXISTS `{CREDENTIAL_NAME}`
COMMENT 'Service principal credential for ADLS Gen2 – {ENV} environment'
"""

# Unity Catalog storage credentials require the service principal details.
# In production, use Managed Identity where supported instead of service principal.
# Ref: https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html

spark.sql(create_credential_sql)
print(f"Storage credential '{CREDENTIAL_NAME}' created or already exists.")

# COMMAND ----------
# MAGIC %md ## 4. Validate Credential

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

validation_df = spark.sql(f"DESCRIBE STORAGE CREDENTIAL `{CREDENTIAL_NAME}`")
validation_df.show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 5. Grant Permissions (adjust principals as needed)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant usage to data engineering group; scope down per environment
# MAGIC -- GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL adls_sp_credential TO `data-engineers`;

# COMMAND ----------
# MAGIC %md ## 6. Output Summary

# COMMAND ----------

print("=" * 60)
print("STORAGE CREDENTIAL SETUP COMPLETE")
print("=" * 60)
print(f"  Credential Name : {CREDENTIAL_NAME}")
print(f"  Environment     : {ENV}")
print(f"  Next Step       : Run nb_env_02_external_locations")
print("=" * 60)
