# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils_data_quality
# MAGIC **Layer:** Utilities (shared across all layers)
# MAGIC **Purpose:** Reusable data quality check framework. Import via `%run` or call as a notebook task.
# MAGIC Logs all results to `audit.dq_check_results`.
# MAGIC
# MAGIC **Usage from other notebooks:**
# MAGIC ```python
# MAGIC %run ../99_utilities/nb_utils_data_quality
# MAGIC results = run_dq_suite(df=my_df, suite=MY_DQ_SUITE, entity_name="orders", layer="silver", run_id=RUN_ID)
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## DQ Check Functions

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from typing import List, Dict, Any, Optional
import re

# ─────────────────────────────────────────────────────────────────────────────
# Core DQ Check Implementations
# ─────────────────────────────────────────────────────────────────────────────

def dq_not_null(df: DataFrame, col: str) -> Dict:
    total = df.count()
    passed = df.filter(F.col(col).isNotNull()).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_unique(df: DataFrame, cols: List[str]) -> Dict:
    total = df.count()
    passed = df.dropDuplicates(cols).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_not_empty_string(df: DataFrame, col: str) -> Dict:
    total = df.count()
    passed = df.filter(
        F.col(col).isNull() | (F.trim(F.col(col)) != "")
    ).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_range(df: DataFrame, col: str, min_val=None, max_val=None) -> Dict:
    total = df.filter(F.col(col).isNotNull()).count()
    cond  = F.lit(True)
    if min_val is not None:
        cond = cond & (F.col(col) >= min_val)
    if max_val is not None:
        cond = cond & (F.col(col) <= max_val)
    passed = df.filter(F.col(col).isNotNull()).filter(cond).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_regex(df: DataFrame, col: str, pattern: str) -> Dict:
    total  = df.filter(F.col(col).isNotNull()).count()
    passed = df.filter(F.col(col).isNotNull()).filter(F.col(col).rlike(pattern)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_accepted_values(df: DataFrame, col: str, accepted: List) -> Dict:
    total  = df.filter(F.col(col).isNotNull()).count()
    passed = df.filter(F.col(col).isin(accepted)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

def dq_referential_integrity(df: DataFrame, col: str, ref_df: DataFrame, ref_col: str) -> Dict:
    """Check every non-null value in col exists in ref_df.ref_col."""
    total   = df.filter(F.col(col).isNotNull()).count()
    orphans = (df
        .filter(F.col(col).isNotNull())
        .join(ref_df.select(F.col(ref_col).alias("__ref")).distinct(),
              F.col(col) == F.col("__ref"), "left_anti")
        .count()
    )
    passed = total - orphans
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": orphans}

def dq_row_count_threshold(df: DataFrame, min_rows: int, max_rows: Optional[int] = None) -> Dict:
    total = df.count()
    failed = 0 if (total >= min_rows and (max_rows is None or total <= max_rows)) else 1
    return {"rows_checked": 1, "rows_passed": 1 - failed, "rows_failed": failed}

def dq_freshness(df: DataFrame, ts_col: str, max_age_hours: int) -> Dict:
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    total  = df.count()
    passed = df.filter(F.col(ts_col) >= F.lit(cutoff)).count()
    return {"rows_checked": total, "rows_passed": passed, "rows_failed": total - passed}

# ─────────────────────────────────────────────────────────────────────────────
# Check Dispatcher
# ─────────────────────────────────────────────────────────────────────────────

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

def run_dq_suite(
    df: DataFrame,
    suite: List[Dict[str, Any]],
    entity_name: str,
    layer: str,
    run_id: str,
    catalog: str,
    batch_date: str,
    fail_on_critical: bool = True,
) -> List[Dict]:
    """
    Run a DQ suite against a DataFrame.

    Parameters
    ----------
    df             : Input DataFrame to validate
    suite          : List of check definitions (see DQ_SUITE_EXAMPLE below)
    entity_name    : e.g. 'orders'
    layer          : e.g. 'bronze', 'silver'
    run_id         : Pipeline run UUID
    catalog        : Unity Catalog name
    batch_date     : YYYY-MM-DD string
    fail_on_critical: Raise RuntimeError if any threshold=1.0 check fails

    Returns
    -------
    List of result dicts (also written to audit.dq_check_results)
    """
    results = []

    for check_cfg in suite:
        check_name = check_cfg.get("name") or f"{check_cfg['type']}__{check_cfg.get('column', 'multi')}"
        check_type = check_cfg["type"]
        threshold  = check_cfg.get("threshold", 0.99)
        col_name   = check_cfg.get("column", "")

        if col_name and col_name not in df.columns:
            print(f"  [SKIP] Column '{col_name}' not in DataFrame — skipping '{check_name}'")
            continue

        fn = CHECK_FNS.get(check_type)
        if not fn:
            print(f"  [SKIP] Unknown check type: {check_type}")
            continue

        try:
            counts     = fn(df, check_cfg)
            rows_chk   = counts["rows_checked"]
            rows_pass  = counts["rows_passed"]
            rows_fail  = counts["rows_failed"]
            pass_rate  = rows_pass / rows_chk if rows_chk > 0 else 1.0
            result     = "PASS" if pass_rate >= threshold else ("FAIL" if threshold == 1.0 else "WARN")

            icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[result]
            print(f"  [{icon}] {result:4s} | {check_name} (pass_rate={pass_rate:.2%}, failed={rows_fail:,})")

            results.append({
                "run_id":       run_id,
                "check_name":   check_name,
                "entity_name":  entity_name,
                "layer":        layer,
                "check_type":   check_type,
                "column_name":  col_name,
                "rows_checked": rows_chk,
                "rows_failed":  rows_fail,
                "pass_rate":    round(pass_rate, 4),
                "threshold":    threshold,
                "result":       result,
                "batch_date":   batch_date,
            })

        except Exception as e:
            print(f"  [ERROR] {check_name}: {e}")
            results.append({
                "run_id": run_id, "check_name": check_name, "entity_name": entity_name,
                "layer": layer, "check_type": check_type, "column_name": col_name,
                "rows_checked": 0, "rows_failed": 0, "pass_rate": 0.0,
                "threshold": threshold, "result": "FAIL", "batch_date": batch_date,
            })

    # Persist results
    if results:
        results_df = spark.createDataFrame(results)
        (results_df.write.format("delta").mode("append")
            .saveAsTable(f"{catalog}.audit.dq_check_results"))

    # Fail pipeline on critical failures
    if fail_on_critical:
        critical = [r for r in results if r["result"] == "FAIL" and r["threshold"] >= 1.0]
        if critical:
            raise RuntimeError(
                f"Critical DQ failures for {entity_name}: {[r['check_name'] for r in critical]}"
            )

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Example DQ Suite — copy and customise in your notebooks
# ─────────────────────────────────────────────────────────────────────────────

DQ_SUITE_EXAMPLE = [
    # Completeness
    {"name": "not_null__order_id",       "type": "not_null",        "column": "order_id",      "threshold": 1.0},
    {"name": "not_null__customer_id",    "type": "not_null",        "column": "customer_id",   "threshold": 1.0},
    {"name": "not_null__order_date",     "type": "not_null",        "column": "order_date",    "threshold": 1.0},

    # Uniqueness
    {"name": "unique__order_id",         "type": "unique",          "column": "order_id",      "threshold": 1.0},

    # Range
    {"name": "range__quantity_positive", "type": "range",           "column": "quantity",      "min": 0,   "threshold": 0.99},
    {"name": "range__unit_price",        "type": "range",           "column": "unit_price",    "min": 0,   "max": 99999, "threshold": 0.99},
    {"name": "range__discount_pct",      "type": "range",           "column": "discount_pct",  "min": 0,   "max": 100, "threshold": 1.0},

    # Accepted values
    {"name": "valid_status",             "type": "accepted_values", "column": "status",
     "values": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "RETURNED", "CANCELLED"], "threshold": 0.99},

    # Format
    {"name": "valid_email",              "type": "regex",           "column": "email",
     "pattern": r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",               "threshold": 0.95},

    # Volume
    {"name": "row_count_min_100",        "type": "row_count_threshold", "min_rows": 100,        "threshold": 1.0},

    # Freshness
    {"name": "freshness_24h",            "type": "freshness",       "column": "created_at",    "max_age_hours": 24, "threshold": 0.95},
]

print("DQ utility functions loaded.")
print("Available check types:", list(CHECK_FNS.keys()))
