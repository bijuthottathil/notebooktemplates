# Databricks notebook source
# MAGIC %md
# MAGIC # nb_env_01_storage_credentials
# MAGIC **Layer:** Environment Setup
# MAGIC **Purpose:** Register Azure service principal credentials in Unity Catalog for ADLS Gen2 access
# MAGIC **Run Order:** 01 → 02 → 03

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# ── Edit these values before running ─────────────────────────────────────────

ENV             = "dev"                  # dev | staging | prod
CREDENTIAL_NAME = "adls_sp_credential"
SECRET_SCOPE    = "kv-databricks"        # Azure Key Vault-backed Databricks secret scope
CLIENT_ID_KEY   = "sp-client-id"         # Secret key name for service principal client ID
CLIENT_SECRET_KEY = "sp-client-secret"   # Secret key name for service principal client secret
TENANT_ID_KEY   = "sp-tenant-id"         # Secret key name for tenant ID

# ─────────────────────────────────────────────────────────────────────────────

print(f"Environment     : {ENV}")
print(f"Credential Name : {CREDENTIAL_NAME}")
print(f"Secret Scope    : {SECRET_SCOPE}")

# COMMAND ----------
# MAGIC %md ## 1. Retrieve Secrets from Azure Key Vault-backed Scope

# COMMAND ----------

client_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_ID_KEY)
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_SECRET_KEY)
tenant_id     = dbutils.secrets.get(scope=SECRET_SCOPE, key=TENANT_ID_KEY)

print("Secrets retrieved successfully.")

# COMMAND ----------
# MAGIC %md ## 2. Create Storage Credential (Unity Catalog)

# COMMAND ----------

spark.sql(f"""
    CREATE STORAGE CREDENTIAL IF NOT EXISTS `{CREDENTIAL_NAME}`
    COMMENT 'Service principal credential for ADLS Gen2 – {ENV} environment'
""")
print(f"Storage credential '{CREDENTIAL_NAME}' created or already exists.")

# COMMAND ----------
# MAGIC %md ## 3. Validate Credential

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

spark.sql(f"DESCRIBE STORAGE CREDENTIAL `{CREDENTIAL_NAME}`").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 4. Grant Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment and adjust principal as needed:
# MAGIC -- GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL adls_sp_credential TO `data-engineers`;

# COMMAND ----------

print("=" * 60)
print("STORAGE CREDENTIAL SETUP COMPLETE")
print("=" * 60)
print(f"  Credential : {CREDENTIAL_NAME}")
print(f"  Environment: {ENV}")
print(f"  Next Step  : Run nb_env_02_external_locations")
print("=" * 60)
