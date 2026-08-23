import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("DataQualityChecks")


def silver_validation(
    table_name: str,
    database: str,
    bronze_table: str,
    silver_table: str,
    silver_failed_table: str,
    silver_schema: Dict[str, str],
    primary_keys: List[str],
    dq_rules: Dict[str, Any],
    expected_bronze_count: int,
    query_executor_fn,
    scalar_executor_fn
) -> Tuple[bool, Dict[str, Any]]:
    """
    Executes comprehensive DQ checks for the Silver layer:
      1. Source-to-Target Volume/Count Comparison vs Bronze Context
      2. Bad / Failed Record Separation into silver.failed
      3. Primary Key Uniqueness / Duplication Verification
      4. Null & Range Check Validation
    """
    validation_passed = True
    dq_report = {
        "table_name": table_name,
        "bronze_expected_count": expected_bronze_count,
        "validations": {}
    }

    # ----------------------------------------------------
    # 1. GENERATE BAD / INVALID RECORD FILTER RULES
    # ----------------------------------------------------
    failure_conditions = []

    # Rule: Null checks on Primary Keys
    for pk in primary_keys:
        failure_conditions.append(f"`{pk}` IS NULL")

    # Rule: Configured Null Checks
    not_null_cols = dq_rules.get("not_null_columns", [])
    for col in not_null_cols:
        failure_conditions.append(f"`{col}` IS NULL")

    # Rule: Range / Custom Expressions
    custom_rules = dq_rules.get("custom_sql_rules", [])
    for rule in custom_rules:
        failure_conditions.append(f"NOT ({rule})")

    where_failed_condition = " OR ".join(failure_conditions) if failure_conditions else "1=0"

    # ----------------------------------------------------
    # 2. SEPARATE BAD RECORDS INTO silver.failed TABLE
    # ----------------------------------------------------
    logger.info(f"Checking and routing bad records to {silver_failed_table}")
    
    insert_failed_sql = f"""
    INSERT INTO {database}.{silver_failed_table}
    SELECT 
        *, 
        '{table_name}' AS failed_table_name,
        CURRENT_TIMESTAMP AS failed_at,
        CASE 
            { ' '.join([f"WHEN {c} THEN '{c}'" for c in failure_conditions]) if failure_conditions else "ELSE 'UNKNOWN'" }
            ELSE 'RULE_VIOLATION'
        END AS failure_reason
    FROM {database}.{bronze_table}
    WHERE {where_failed_condition}
    """
    query_executor_fn(insert_failed_sql, f"Route bad records to {silver_failed_table}")

    # Count failed records
    failed_count_sql = f"SELECT COUNT(*) FROM {database}.{silver_failed_table} WHERE failed_table_name = '{table_name}'"
    failed_records_count = int(scalar_executor_fn(failed_count_sql, "Count failed records"))
    dq_report["validations"]["failed_records_count"] = failed_records_count

    # ----------------------------------------------------
    # 3. VERIFY SILVER RECORD COUNT & VOLUME COMPARISON
    # ----------------------------------------------------
    silver_count_sql = f"SELECT COUNT(*) FROM {database}.{silver_table}"
    actual_silver_count = int(scalar_executor_fn(silver_count_sql, "Count silver records"))
    dq_report["validations"]["actual_silver_count"] = actual_silver_count

    # Compare Context Bronze count against (Silver + Failed)
    total_accounted = actual_silver_count + failed_records_count
    if expected_bronze_count > 0 and total_accounted < expected_bronze_count:
        validation_passed = False
        dq_report["validations"]["count_check"] = {
            "status": "FAILED",
            "message": f"Volume mismatch: Bronze Context ({expected_bronze_count}) > Silver ({actual_silver_count}) + Failed ({failed_records_count})"
        }
    else:
        dq_report["validations"]["count_check"] = {
            "status": "PASSED",
            "message": f"Processed {actual_silver_count} valid records and {failed_records_count} bad records against {expected_bronze_count} Bronze baseline."
        }

    # ----------------------------------------------------
    # 4. PRIMARY KEY DUPLICATION CHECK IN SILVER
    # ----------------------------------------------------
    if primary_keys:
        pk_cols_str = ", ".join([f"`{pk}`" for pk in primary_keys])
        pk_dup_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT {pk_cols_str}, COUNT(*) as dup_cnt
            FROM {database}.{silver_table}
            GROUP BY {pk_cols_str}
            HAVING COUNT(*) > 1
        )
        """
        duplicate_pk_count = int(scalar_executor_fn(pk_dup_sql, "Check duplicate primary keys"))
        dq_report["validations"]["pk_duplicates"] = duplicate_pk_count

        if duplicate_pk_count > 0:
            validation_passed = False
            dq_report["validations"]["pk_check"] = {
                "status": "FAILED",
                "message": f"Found {duplicate_pk_count} duplicated primary key combinations in {silver_table}."
            }
        else:
            dq_report["validations"]["pk_check"] = {"status": "PASSED"}

    dq_report["status"] = "PASSED" if validation_passed else "FAILED"
    return validation_passed, dq_report