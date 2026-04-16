# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils_data_quality
# MAGIC **Layer:** Utilities
# MAGIC **Purpose:** Reusable DQ check framework. Import via `%run` from any notebook.
# MAGIC
# MAGIC **Usage:**
# MAGIC ```python
# MAGIC %run ./utilities/nb_utils_data_quality
# MAGIC results = run_dq_suite(df=my_df, suite=MY_DQ_SUITE, entity_name="claims",
# MAGIC                        layer="bronze", run_id=RUN_ID, catalog=CATALOG, batch_date=BATCH_DATE)
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Individual Check Functions
# ─────────────────────────────────────────────────────────────────────────────

def dq_not_null(df, col):
    total  = df.count()
    passed = df.filter(F.col(col).isNotNull()).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_unique(df, cols):
    total  = df.count()
    passed = df.dropDuplicates(cols).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_not_empty_string(df, col):
    total  = df.count()
    passed = df.filter(F.col(col).isNull() | (F.trim(F.col(col)) != "")).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_range(df, col, min_val=None, max_val=None):
    base  = df.filter(F.col(col).isNotNull())
    total = base.count()
    cond  = F.lit(True)
    if min_val is not None:
        cond = cond & (F.col(col) >= min_val)
    if max_val is not None:
        cond = cond & (F.col(col) <= max_val)
    passed = base.filter(cond).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_regex(df, col, pattern):
    base   = df.filter(F.col(col).isNotNull())
    total  = base.count()
    passed = base.filter(F.col(col).rlike(pattern)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_accepted_values(df, col, accepted):
    total  = df.filter(F.col(col).isNotNull()).count()
    passed = df.filter(F.col(col).isin(accepted)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_row_count_threshold(df, min_rows, max_rows=None):
    total = df.count()
    ok    = total >= min_rows and (max_rows is None or total <= max_rows)
    return {"rows_checked": 1, "rows_passed": int(ok), "rows_failed": int(not ok)}

def dq_freshness(df, ts_col, max_age_hours):
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    total  = df.count()
    passed = df.filter(F.col(ts_col) >= F.lit(cutoff)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

CHECK_FNS = {
    "not_null":            lambda df, cfg: dq_not_null(df, cfg["column"]),
    "unique":              lambda df, cfg: dq_unique(df, cfg.get("columns", [cfg["column"]])),
    "not_empty_string":    lambda df, cfg: dq_not_empty_string(df, cfg["column"]),
    "range":               lambda df, cfg: dq_range(df, cfg["column"], cfg.get("min"), cfg.get("max")),
    "regex":               lambda df, cfg: dq_regex(df, cfg["column"], cfg["pattern"]),
    "accepted_values":     lambda df, cfg: dq_accepted_values(df, cfg["column"], cfg["values"]),
    "row_count_threshold": lambda df, cfg: dq_row_count_threshold(df, cfg["min_rows"], cfg.get("max_rows")),
    "freshness":           lambda df, cfg: dq_freshness(df, cfg["column"], cfg["max_age_hours"]),
}

# ─────────────────────────────────────────────────────────────────────────────
# Suite Runner
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_suite(df, suite, entity_name, layer, run_id, catalog, batch_date, fail_on_critical=True):
    results = []
    for check_cfg in suite:
        check_name = check_cfg.get("name") or f"{check_cfg['type']}__{check_cfg.get('column','multi')}"
        check_type = check_cfg["type"]
        threshold  = check_cfg.get("threshold", 0.99)
        col_name   = check_cfg.get("column", "")

        if col_name and col_name not in df.columns:
            print(f"  [SKIP] {col_name} not in DataFrame")
            continue

        fn = CHECK_FNS.get(check_type)
        if not fn:
            print(f"  [SKIP] Unknown check type: {check_type}")
            continue

        try:
            counts    = fn(df, check_cfg)
            rows_chk  = counts["rows_checked"]
            rows_fail = counts["rows_failed"]
            pass_rate = counts["rows_passed"] / rows_chk if rows_chk > 0 else 1.0
            result    = "PASS" if pass_rate >= threshold else ("FAIL" if threshold == 1.0 else "WARN")
            icon      = {"PASS": "[OK]", "WARN": "[WARN]", "FAIL": "[FAIL]"}[result]
            print(f"  {icon} {result:4s} | {check_name} (pass_rate={pass_rate:.2%}, failed={rows_fail:,})")

            results.append({
                "run_id": run_id, "check_name": check_name, "entity_name": entity_name,
                "layer": layer, "check_type": check_type, "column_name": col_name,
                "rows_checked": rows_chk, "rows_failed": rows_fail,
                "pass_rate": round(pass_rate, 4), "threshold": threshold,
                "result": result, "batch_date": batch_date,
            })
        except Exception as e:
            print(f"  [ERROR] {check_name}: {e}")
            results.append({
                "run_id": run_id, "check_name": check_name, "entity_name": entity_name,
                "layer": layer, "check_type": check_type, "column_name": col_name,
                "rows_checked": 0, "rows_failed": 0, "pass_rate": 0.0,
                "threshold": threshold, "result": "FAIL", "batch_date": batch_date,
            })

    if results:
        spark.createDataFrame(results).write.format("delta").mode("append") \
            .saveAsTable(f"{catalog}.audit.dq_check_results")

    if fail_on_critical:
        critical = [r for r in results if r["result"] == "FAIL" and r["threshold"] >= 1.0]
        if critical:
            raise RuntimeError(f"Critical DQ failures for {entity_name}: {[r['check_name'] for r in critical]}")

    return results


print("DQ utility loaded. Check types:", list(CHECK_FNS.keys()))
