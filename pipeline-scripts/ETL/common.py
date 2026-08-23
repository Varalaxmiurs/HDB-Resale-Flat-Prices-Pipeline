"""
common.py
=========
Shared helper functions for the HDB Resale Flat Prices pipeline. Imported
by every Glue job script (job_1 .. job_5) so that Iceberg read/write,
surrogate-key, audit-logging, and alerting logic is defined once.

All configurable values (Glue database, Athena workgroup, S3 buckets, the
SNS topic ARN, the natural key columns, etc.) live in config.py - the
pipeline's single source of truth - and are imported here rather than
duplicated. See config.py's docstring: hardcoding one of those values
anywhere else is considered a bug.

All 5 jobs + the orchestration notebook assume these Iceberg tables already
exist (or will be auto-created on first write) in the Glue Data Catalog:

    raw_iceberg          - combined raw data, minimal transformation
    cleaned_iceberg       - passed Part 1 data-quality validation rules
    transformed_iceberg   - + Resale Identifier column
    hashed_iceberg        - + hashed Resale Identifier column
    failed_iceberg        - quarantine: any record rejected by cleaned,
                             transformed, or hashed stages, tagged with the
                             reason and the stage that rejected it
"""

import hashlib
import logging
import time
import uuid
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd

from config import ATHENA_WORKGROUP, GLUE_DATABASE, NATURAL_KEY_COLUMNS, SNS_TOPIC_ARN, TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# --------------------------------------------------------------------------- #
# Surrogate key
# --------------------------------------------------------------------------- #

def compute_surrogate_key(df: pd.DataFrame, key_columns: list = None) -> pd.Series:
    """
    Deterministic SHA-256 surrogate key from the natural/composite key
    columns. The SAME input row always yields the SAME key - this is what
    makes MERGE-based upserts idempotent across reruns: replaying identical
    source data updates the same rows in place instead of duplicating them.
    """
    key_columns = key_columns or [c for c in NATURAL_KEY_COLUMNS if c in df.columns]
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise KeyError(f"Cannot compute surrogate key - missing columns: {missing}")
    concat = df[key_columns].astype(str).agg("|".join, axis=1)
    return concat.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())


# --------------------------------------------------------------------------- #
# Iceberg read / write
# --------------------------------------------------------------------------- #

def write_iceberg(df: pd.DataFrame, stage: str, mode: str = "append") -> None:
    """Write a dataframe to the named Iceberg stage table. No-op on empty df.

    NOTE: plain append - only safe for tables where every write is genuinely
    new data (e.g. failed_iceberg, audit_iceberg, or hashed_iceberg's SCD2
    versions, which are already deduplicated by apply_scd2 before this is
    called). For raw/cleaned/transformed, use merge_iceberg() instead so
    reruns don't create duplicate rows.
    """
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    wr.athena.to_iceberg(
        df=df,
        database=GLUE_DATABASE,
        table=table,
        table_location=location,
        keep_files=False,
        mode=mode,
    )


def execute_athena_sql(sql: str, description: str) -> None:
    """Run an arbitrary Athena SQL statement (DDL/DML) and block until done.

    Needed for MERGE/UPDATE against Iceberg tables, which awswrangler's
    to_iceberg() dataframe writer doesn't support directly. Requires an
    Athena workgroup on engine version 3.
    """
    athena = boto3.client("athena")
    logger = get_logger("athena_sql")
    logger.info("Athena SQL: %s", description)

    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            logger.info("Athena SQL succeeded: %s", description)
            return
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena SQL failed ({description}): {reason}")
        time.sleep(2)


def merge_iceberg(df: pd.DataFrame, stage: str, key_column: str = "surrogate_key") -> None:
    """
    Idempotent upsert: write df to a staging Iceberg table, then MERGE INTO
    the target table on key_column. Same input on a rerun -> matched rows
    get UPDATEd in place (no duplication); genuinely new rows get INSERTed.
    This is what actually makes raw/cleaned/transformed idempotent, as
    opposed to write_iceberg()'s plain append.
    """
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    staging_table = f"{table}_staging"
    staging_location = location.rstrip("/") + "_staging/"

    wr.athena.to_iceberg(
        df=df,
        database=GLUE_DATABASE,
        table=staging_table,
        table_location=staging_location,
        keep_files=True,
        mode="overwrite",  # staging table only ever holds this run's batch
    )

    all_cols = [c for c in df.columns if c != key_column]
    update_set = ", ".join(f"t.{c} = s.{c}" for c in all_cols)
    insert_cols = ", ".join([key_column] + all_cols)
    insert_vals = ", ".join([f"s.{key_column}"] + [f"s.{c}" for c in all_cols])

    merge_sql = f"""
    MERGE INTO {GLUE_DATABASE}.{table} t
    USING {GLUE_DATABASE}.{staging_table} s
    ON t.{key_column} = s.{key_column}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    execute_athena_sql(merge_sql, f"Idempotent upsert into {table} ({len(df)} rows) via {staging_table}")


def read_iceberg(stage: str) -> pd.DataFrame:
    """Read the full contents of a stage's Iceberg table via Athena."""
    table, _ = TABLES[stage]
    return wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{table}"',
        database=GLUE_DATABASE,
        ctas_approach=False,
    )


def route_to_failed(df: pd.DataFrame, reason: str, stage: str) -> None:
    """Tag rejected records with reason + originating stage and append to failed_iceberg."""
    if df is None or df.empty:
        return
    tagged = df.copy()
    tagged["_failure_reason"] = reason
    tagged["_failed_stage"] = stage
    tagged["_failed_at"] = datetime.utcnow().isoformat()
    write_iceberg(tagged, "failed")


# --------------------------------------------------------------------------- #
# Audit logging (hdb-eventdriven-audit-tables)
# --------------------------------------------------------------------------- #

def record_audit(job_name: str, stage: str, rows_in: int, rows_out: int, rows_rejected: int) -> None:
    """
    Append one row per job run to the audit_iceberg table: what ran, how many
    rows came in, how many passed, how many were rejected. This is the
    traceability trail referenced by hdb-eventdriven-audit-tables - separate
    from failed_iceberg, which holds the actual rejected records themselves.
    """
    audit_row = pd.DataFrame([{
        "run_id": str(uuid.uuid4()),
        "job_name": job_name,
        "stage": stage,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "run_timestamp": datetime.utcnow().isoformat(),
    }])
    write_iceberg(audit_row, stage="audit", mode="append")


# --------------------------------------------------------------------------- #
# Alerting
# --------------------------------------------------------------------------- #

def send_alert(subject: str, message: str) -> None:
    """Publish a run summary / failure alert to SNS."""
    sns = boto3.client("sns")
    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
