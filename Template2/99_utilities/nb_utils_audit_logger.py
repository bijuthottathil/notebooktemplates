# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils_audit_logger
# MAGIC **Layer:** Utilities
# MAGIC **Purpose:** Centralised pipeline audit logging context manager.
# MAGIC Import via `%run` from any bronze/silver/gold notebook.
# MAGIC
# MAGIC **Usage:**
# MAGIC ```python
# MAGIC %run ../99_utilities/nb_utils_audit_logger
# MAGIC
# MAGIC with PipelineAudit(catalog=CATALOG, pipeline_name="nb_brz_01_full_load_batch",
# MAGIC                    layer="bronze", source_system="crm", entity_name="customers",
# MAGIC                    batch_date=BATCH_DATE, load_type="full") as audit:
# MAGIC     df = spark.read...
# MAGIC     audit.set_rows_read(df.count())
# MAGIC     df.write...
# MAGIC     audit.set_rows_written(rows_written)
# MAGIC ```

# COMMAND ----------

import datetime, uuid, traceback

class PipelineAudit:
    """Context manager for automatic pipeline audit logging."""

    def __init__(self, catalog, pipeline_name, layer, source_system,
                 entity_name, batch_date, load_type, run_id=None):
        self.catalog       = catalog
        self.pipeline_name = pipeline_name
        self.layer         = layer
        self.source_system = source_system
        self.entity_name   = entity_name
        self.batch_date    = batch_date
        self.load_type     = load_type
        self.run_id        = run_id or str(uuid.uuid4())
        self.start_ts      = None
        self._rows_read    = 0
        self._rows_written = 0
        self._rows_rejected= 0
        try:
            self.notebook_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().notebookPath().get()
            )
        except Exception:
            self.notebook_path = "unknown"

    def set_rows_read(self, n):     self._rows_read     = n
    def set_rows_written(self, n):  self._rows_written  = n
    def set_rows_rejected(self, n): self._rows_rejected = n

    def _log(self, status, error_msg=""):
        elapsed   = (datetime.datetime.utcnow() - self.start_ts).total_seconds() if self.start_ts else 0
        safe_err  = error_msg.replace("'", "''")[:2000]
        try:
            spark.sql(f"""
                MERGE INTO `{self.catalog}`.`audit`.`pipeline_run_log` AS t
                USING (SELECT
                    '{self.run_id}'        AS run_id,
                    '{self.pipeline_name}' AS pipeline_name,
                    '{self.layer}'         AS layer,
                    '{self.source_system}' AS source_system,
                    '{self.entity_name}'   AS entity_name,
                    '{self.batch_date}'    AS batch_date,
                    '{self.load_type}'     AS load_type,
                    '{status}'             AS status,
                    {self._rows_read}      AS rows_read,
                    {self._rows_written}   AS rows_written,
                    {self._rows_rejected}  AS rows_rejected,
                    current_timestamp()    AS end_ts,
                    {elapsed:.2f}          AS duration_seconds,
                    '{safe_err}'           AS error_message,
                    '{self.notebook_path}' AS notebook_path
                ) AS s ON t.run_id = s.run_id
                WHEN MATCHED THEN UPDATE SET
                    t.status=s.status, t.rows_read=s.rows_read, t.rows_written=s.rows_written,
                    t.rows_rejected=s.rows_rejected, t.end_ts=s.end_ts,
                    t.duration_seconds=s.duration_seconds, t.error_message=s.error_message
                WHEN NOT MATCHED THEN INSERT *
            """)
        except Exception as e:
            print(f"[AUDIT WARNING] Failed to write audit log: {e}")

    def __enter__(self):
        self.start_ts = datetime.datetime.utcnow()
        try:
            spark.sql(f"""
                INSERT INTO `{self.catalog}`.`audit`.`pipeline_run_log`
                (run_id, pipeline_name, layer, source_system, entity_name, batch_date,
                 load_type, status, rows_read, rows_written, rows_rejected,
                 start_ts, end_ts, duration_seconds, error_message, notebook_path)
                VALUES ('{self.run_id}','{self.pipeline_name}','{self.layer}',
                        '{self.source_system}','{self.entity_name}','{self.batch_date}',
                        '{self.load_type}','RUNNING',0,0,0,
                        current_timestamp(),NULL,NULL,NULL,'{self.notebook_path}')
            """)
        except Exception as e:
            print(f"[AUDIT WARNING] Could not log start: {e}")
        print(f"[AUDIT] Started — run_id: {self.run_id}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._log("SUCCESS")
            elapsed = (datetime.datetime.utcnow() - self.start_ts).total_seconds()
            print(f"[AUDIT] SUCCESS — {self._rows_written:,} rows in {elapsed:.1f}s")
        else:
            self._log("FAILED", error_msg=str(exc_val))
            print(f"[AUDIT] FAILED — {exc_val}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def show_pipeline_history(catalog, pipeline_name=None, days=7):
    where = f"WHERE start_ts >= current_timestamp() - INTERVAL {days} DAYS"
    if pipeline_name:
        where += f" AND pipeline_name = '{pipeline_name}'"
    return spark.sql(f"""
        SELECT pipeline_name, layer, entity_name, batch_date, status,
               rows_read, rows_written, ROUND(duration_seconds,1) AS duration_s,
               start_ts, error_message
        FROM `{catalog}`.`audit`.`pipeline_run_log`
        {where} ORDER BY start_ts DESC LIMIT 50
    """)

def show_failed_runs(catalog, days=1):
    return spark.sql(f"""
        SELECT run_id, pipeline_name, layer, entity_name, batch_date, error_message, start_ts
        FROM `{catalog}`.`audit`.`pipeline_run_log`
        WHERE status = 'FAILED'
          AND start_ts >= current_timestamp() - INTERVAL {days} DAYS
        ORDER BY start_ts DESC
    """)

print("Audit logger loaded.")
