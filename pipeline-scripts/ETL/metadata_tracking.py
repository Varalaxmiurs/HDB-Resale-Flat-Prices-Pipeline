"""
metadata_tracking.py
=====================
Watermark + pipeline-run tracking for the HDB Iceberg pipeline, backed by
the metadata tables created by setup_metadata.py:

    table_watermarks  - one row per table_id: the last successfully
                         processed watermark value (for incremental loads)
                         and when/which run last advanced it.
    pipeline_runs      - one row per job execution: start/end time, status
                         (RUNNING / SUCCESS / FAILED), record count, and
                         error message if any.

Usage pattern - wrap a job's actual work in run_with_tracking():

    from metadata_tracking import run_with_tracking

    def do_the_actual_work(old_watermark):
        # old_watermark is whatever was last stored (e.g. a max 'month'
        # value, a timestamp, a file cursor) - use it to decide what to
        # process this run, or ignore it for a full reload.
        ...
        records_processed = 12345
        new_watermark_value = "2016-12"
        return records_processed, new_watermark_value

    run_with_tracking(table_id=2, layer="raw", job_fn=do_the_actual_work)

What run_with_tracking() does, in order:
    1. Reads the current watermark for table_id from table_watermarks.
    2. Inserts a RUNNING row into pipeline_runs.
    3. Calls job_fn(old_watermark).
    4. On success: UPDATEs pipeline_runs to SUCCESS, MERGEs the new
       watermark value into table_watermarks, builds an execution summary,
       emails it.
    5. On failure: UPDATEs pipeline_runs to FAILED with the error message,
       leaves the watermark untouched, builds a summary, emails it, then
       re-raises the exception so the Glue job itself still shows FAILED
       in the AWS console.
"""

import logging
import random
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

import boto3

from config import (
    AWS_REGION,
    METADATA_ATHENA_WORKGROUP,
    METADATA_GLUE_DATABASE,
    SES_RECIPIENT_EMAILS,
    SES_SENDER_EMAIL,
)

DATABASE = METADATA_GLUE_DATABASE
WORKGROUP = METADATA_ATHENA_WORKGROUP
REGION = AWS_REGION

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

athena = boto3.client("athena", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)


def _now_athena_ts() -> str:
    """Athena timestamp literal format: 'YYYY-MM-DD HH:MI:SS.fff' (no 'T', no offset)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# --------------------------------------------------------------------------- #
# Athena query helpers (same start/poll pattern as setup_metadata.py)
# --------------------------------------------------------------------------- #

def run_query(sql: str, description: str) -> str:
    logger.info("Running query: %s", description)
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    query_execution_id = response["QueryExecutionId"]
    logger.info("Query started: %s", query_execution_id)
    return query_execution_id


def wait_for_query(query_execution_id: str) -> None:
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            logger.info("Query succeeded: %s", query_execution_id)
            return
        if status in ("FAILED", "CANCELLED"):
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            logger.error("Query failed: %s", reason)
            raise Exception(reason)
        time.sleep(2)


def execute_query(sql: str, description: str) -> str:
    query_id = run_query(sql, description)
    wait_for_query(query_id)
    return query_id


def fetch_query_results(query_execution_id: str) -> list:
    """Return SELECT results as a list of dicts (first row = header, per Athena's API)."""
    rows = []
    header = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        for row in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return rows


# --------------------------------------------------------------------------- #
# Watermark read
# --------------------------------------------------------------------------- #

def get_watermark(table_id: int) -> Tuple[Optional[str], Optional[str]]:
    """Return (last_watermark_value, last_updated_at) for table_id, or (None, None) on first run."""
    sql = f"""
    SELECT last_watermark_value, last_updated_at
    FROM {DATABASE}.table_watermarks
    WHERE table_id = {table_id}
    ORDER BY last_updated_at DESC
    LIMIT 1
    """
    query_id = execute_query(sql, f"Read watermark for table_id={table_id}")
    rows = fetch_query_results(query_id)

    if not rows:
        logger.info("No existing watermark for table_id=%s - treating as first run", table_id)
        return None, None

    row = rows[0]
    logger.info(
        "Watermark for table_id=%s: value=%s, last_updated_at=%s",
        table_id, row["last_watermark_value"], row["last_updated_at"],
    )
    return row["last_watermark_value"], row["last_updated_at"]


# --------------------------------------------------------------------------- #
# pipeline_runs: start / complete
# --------------------------------------------------------------------------- #

def _new_run_id() -> int:
    # BIGINT run_id: millisecond timestamp + random suffix - unique enough, roughly time-ordered
    return int(time.time() * 1000) * 1000 + random.randint(0, 999)


def start_pipeline_run(table_id: int, layer: str) -> Tuple[int, str]:
    run_id = _new_run_id()
    start_time = _now_athena_ts()

    sql = f"""
    INSERT INTO {DATABASE}.pipeline_runs
        (run_id, table_id, layer, start_time, end_time, status, number_of_records, error_message)
    VALUES
        ({run_id}, {table_id}, '{layer}', TIMESTAMP '{start_time}', NULL, 'RUNNING', NULL, NULL)
    """
    execute_query(sql, f"Start pipeline_run run_id={run_id} table_id={table_id} layer={layer}")
    logger.info("pipeline_runs: run_id=%s marked RUNNING", run_id)
    return run_id, start_time


def complete_pipeline_run(
    run_id: int,
    status: str,  # 'SUCCESS' or 'FAILED'
    number_of_records: Optional[int] = None,
    error_message: Optional[str] = None,
) -> str:
    end_time = _now_athena_ts()

    number_of_records_sql = "NULL" if number_of_records is None else str(number_of_records)
    error_message_sql = (
        "NULL" if not error_message else "'" + error_message.replace("'", "''")[:1000] + "'"
    )

    sql = f"""
    UPDATE {DATABASE}.pipeline_runs
    SET end_time = TIMESTAMP '{end_time}',
        status = '{status}',
        number_of_records = {number_of_records_sql},
        error_message = {error_message_sql}
    WHERE run_id = {run_id}
    """
    execute_query(sql, f"Complete pipeline_run run_id={run_id} status={status}")
    logger.info("pipeline_runs: run_id=%s marked %s", run_id, status)
    return end_time


# --------------------------------------------------------------------------- #
# table_watermarks: update (only ever called on SUCCESS)
# --------------------------------------------------------------------------- #

def update_watermark(table_id: int, new_watermark_value: str, run_id: int) -> None:
    now = _now_athena_ts()
    safe_value = new_watermark_value.replace("'", "''")

    sql = f"""
    MERGE INTO {DATABASE}.table_watermarks t
    USING (
        SELECT {table_id} AS table_id, '{safe_value}' AS last_watermark_value,
               TIMESTAMP '{now}' AS last_updated_at, {run_id} AS last_run_id
    ) s
    ON t.table_id = s.table_id
    WHEN MATCHED THEN UPDATE SET
        last_watermark_value = s.last_watermark_value,
        last_updated_at = s.last_updated_at,
        last_run_id = s.last_run_id
    WHEN NOT MATCHED THEN INSERT
        (table_id, last_watermark_value, last_updated_at, last_run_id)
        VALUES (s.table_id, s.last_watermark_value, s.last_updated_at, s.last_run_id)
    """
    execute_query(sql, f"Update watermark for table_id={table_id}")
    logger.info("table_watermarks: table_id=%s advanced to %s", table_id, new_watermark_value)


# --------------------------------------------------------------------------- #
# Execution summary + email
# --------------------------------------------------------------------------- #

def build_execution_summary(
    run_id: int, table_id: int, layer: str, status: str,
    start_time: str, end_time: str, number_of_records: Optional[int],
    old_watermark: Optional[str], new_watermark: Optional[str],
    error_message: Optional[str],
) -> str:
    lines = [
        "HDB Pipeline Execution Summary",
        "================================",
        f"Run ID           : {run_id}",
        f"Table ID         : {table_id}",
        f"Layer            : {layer}",
        f"Status           : {status}",
        f"Start Time (UTC) : {start_time}",
        f"End Time (UTC)   : {end_time}",
        f"Records Processed: {number_of_records if number_of_records is not None else 'N/A'}",
        f"Watermark Before : {old_watermark or 'N/A (first run)'}",
        f"Watermark After  : {new_watermark if status == 'SUCCESS' else '(unchanged - run failed)'}",
    ]
    if error_message:
        lines += ["", "Error:", error_message]
    return "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    try:
        ses.send_email(
            Source=SES_SENDER_EMAIL,
            Destination={"ToAddresses": SES_RECIPIENT_EMAILS},
            Message={
                "Subject": {"Data": subject[:100]},
                "Body": {"Text": {"Data": body}},
            },
        )
        logger.info("Email sent: %s", subject)
    except Exception:
        # An email failure must never mask the underlying pipeline result.
        logger.exception("Failed to send notification email (subject=%s)", subject)


# --------------------------------------------------------------------------- #
# Orchestrating wrapper
# --------------------------------------------------------------------------- #

def run_with_tracking(
    table_id: int,
    layer: str,
    job_fn: Callable[[Optional[str]], Tuple[int, str]],
) -> None:
    """
    job_fn receives the current watermark value (or None on first run) and
    must return (number_of_records_processed, new_watermark_value).
    """
    old_watermark, _ = get_watermark(table_id)
    run_id, start_time = start_pipeline_run(table_id, layer)

    try:
        number_of_records, new_watermark = job_fn(old_watermark)
    except Exception as exc:
        end_time = complete_pipeline_run(run_id, status="FAILED", error_message=str(exc))
        summary = build_execution_summary(
            run_id, table_id, layer, "FAILED", start_time, end_time,
            None, old_watermark, None, str(exc),
        )
        send_email(f"[FAILED] HDB pipeline - {layer} (table_id={table_id})", summary)
        raise  # let the Glue job itself surface as failed in the AWS console

    end_time = complete_pipeline_run(run_id, status="SUCCESS", number_of_records=number_of_records)
    update_watermark(table_id, new_watermark, run_id)

    summary = build_execution_summary(
        run_id, table_id, layer, "SUCCESS", start_time, end_time,
        number_of_records, old_watermark, new_watermark, None,
    )
    send_email(f"[SUCCESS] HDB pipeline - {layer} (table_id={table_id})", summary)
