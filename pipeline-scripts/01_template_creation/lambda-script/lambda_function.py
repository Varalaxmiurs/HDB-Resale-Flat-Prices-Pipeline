"""
lambda_function.py
===================
ONE Lambda, reused for two purposes against the 4 metadata Iceberg tables
(metadata_tables, table_parameters, table_watermarks, pipeline_runs):

  - "read"   (default) - just read all 4 tables and return them as JSON.
  - "update" - apply watermark/pipeline_run updates first, THEN read all 4
               tables and return the fresh JSON - so either action always
               hands the caller something to pass forward.

Meant to be invoked SYNCHRONOUSLY from pipeline_orchestration.ipynb, e.g.:

    # just read current state
    resp = lambda_client.invoke(
        FunctionName="hdb-eventdriven-metadata-reader",
        InvocationType="RequestResponse",
        Payload=json.dumps({"action": "read"}).encode(),
    )
    metadata = json.loads(resp["Payload"].read())

    # bump a watermark + log a run, get the updated state back
    resp = lambda_client.invoke(
        FunctionName="hdb-eventdriven-metadata-reader",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "action": "update",
            "watermark_updates": [
                {"table_id": 1, "last_watermark_value": "2026-08-23T00:00:00", "last_run_id": 101},
            ],
            "pipeline_run": {
                "run_id": 101, "table_id": 1, "layer": "bronze",
                "start_time": "2026-08-23T00:50:00", "end_time": "2026-08-23T00:53:00",
                "status": "SUCCEEDED", "number_of_records": 3, "error_message": None,
            },
        }).encode(),
    )
    metadata = json.loads(resp["Payload"].read())
    # metadata["tables"] is now the metadata state a following cell can act on

Either call's response has the same shape (see lambda_handler's return), which
is the point of reusing one function instead of a separate read-only /
write-only pair: the calling cell doesn't need to branch on which action it
sent - it just reads response["tables"].

Deployed by setup.sh Step 10 as "${PROJECT_NAME}-metadata-reader", using the
IAM role setup.sh Step 5 creates (${PROJECT_NAME}-lambda-role) - that role
needs AmazonAthenaFullAccess + AWSGlueConsoleFullAccess in addition to the
basic execution role it already had, since this function talks to
Athena/Glue directly via boto3 rather than through awswrangler (Lambda's
default runtime doesn't have awswrangler installed, and adding a layer just
for a handful of read/write statements isn't worth the extra deployment
complexity - plain boto3 + Athena's start_query_execution/get_query_results
is enough, mirroring the same hand-rolled pattern already used in
01_metadata_setup.py).

Environment variables (set by setup.sh at deploy time):
    GLUE_DATABASE     - Glue/Athena database the metadata tables live in
    ATHENA_WORKGROUP  - Athena workgroup to run queries in (must be engine v3
                        for Iceberg UPDATE/INSERT)
    AUDIT_BUCKET      - bucket holding the metadata tables' data +
                        athena-results/ (used as the query output location)
    AWS_REGION_NAME   - region to run Athena/Glue calls in. NOT named
                        AWS_REGION - that name is reserved by the Lambda
                        runtime itself and can't be set as a user env var.
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = os.environ.get("AWS_REGION_NAME", "us-east-1")
DATABASE = os.environ["GLUE_DATABASE"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]
AUDIT_BUCKET = os.environ["AUDIT_BUCKET"]
RESULTS_LOCATION = f"s3://{AUDIT_BUCKET}/athena-results/"

METADATA_TABLES = ["metadata_tables", "table_parameters", "table_watermarks", "pipeline_runs"]

QUERY_POLL_SECONDS = 1
QUERY_TIMEOUT_SECONDS = 45  # stay comfortably under the Lambda's own --timeout (60s)

athena = boto3.client("athena", region_name=REGION)


# --------------------------------------------------------------------------- #
# Athena query/DML execution
# --------------------------------------------------------------------------- #

def _run_statement(sql: str) -> str:
    """Run any Athena SQL (SELECT, UPDATE, or INSERT) and block until done."""
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": RESULTS_LOCATION},
    )["QueryExecutionId"]

    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            return query_id
        if status in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Query failed ({sql.strip()[:80]}...): {reason}")
        time.sleep(QUERY_POLL_SECONDS)

    raise TimeoutError(f"Query timed out after {QUERY_TIMEOUT_SECONDS}s: {sql.strip()[:80]}...")


def _fetch_rows_as_dicts(query_id: str) -> list:
    rows = []
    columns = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        result_rows = page["ResultSet"]["Rows"]
        if columns is None:
            # Only the very first row of the very first page is the header -
            # strip it once, not on every page.
            columns = [c.get("VarCharValue") for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            values = [c.get("VarCharValue") for c in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


def _sql_literal(value) -> str:
    """Render a Python value as an Athena SQL literal (quoted string, NULL, or bare number)."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _bigint_literal(value) -> str:
    """A bare Python int literal (e.g. "1") infers as Athena's INTEGER type,
    not BIGINT - table_watermarks.last_run_id and pipeline_runs' run_id/
    table_id/number_of_records are all BIGINT (01_metadata_setup.py), and
    Athena's Iceberg INSERT VALUES enforces an EXACT column-type match, so
    an un-cast integer literal there raises TYPE_MISMATCH ("Query: [...,
    integer, ...]" vs "Table: [..., bigint, ...]"). Casting explicitly
    sidesteps Athena's own type-inference instead of relying on it."""
    if value is None:
        return "NULL"
    return f"CAST({_sql_literal(value)} AS BIGINT)"


def _timestamp_literal(value) -> str:
    """A bare quoted ISO datetime string (e.g. '2026-08-24T06:57:18.887661')
    infers as VARCHAR, not TIMESTAMP - same INSERT-strictness problem as
    _bigint_literal() above, for pipeline_runs.start_time/end_time (both
    TIMESTAMP). Athena's TIMESTAMP literal syntax wants a space instead of
    the ISO 'T' separator and no trailing 'Z' - both stripped here."""
    if value is None:
        return "NULL"
    normalized = str(value).replace("T", " ")
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    return f"TIMESTAMP {_sql_literal(normalized)}"


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #

def _read_all_tables() -> tuple:
    tables = {}
    errors = {}
    for table in METADATA_TABLES:
        try:
            query_id = _run_statement(f'SELECT * FROM "{DATABASE}"."{table}"')
            tables[table] = _fetch_rows_as_dicts(query_id)
        except Exception as exc:  # noqa: BLE001 - one bad table shouldn't
            # block the other 3 from coming back; the caller sees exactly
            # which table(s) failed and why via "errors".
            errors[table] = str(exc)
    return tables, errors


# --------------------------------------------------------------------------- #
# Update
# --------------------------------------------------------------------------- #

def _update_watermark(update: dict) -> None:
    """
    update = {"table_id": 1, "last_watermark_value": "...", "last_run_id": 101}
    Mirrors table_watermarks' schema from 01_metadata_setup.py.
    """
    table_id = update["table_id"]
    set_clauses = ["last_updated_at = current_timestamp"]
    if "last_watermark_value" in update:
        set_clauses.append(f"last_watermark_value = {_sql_literal(update['last_watermark_value'])}")
    if "last_run_id" in update:
        set_clauses.append(f"last_run_id = {_bigint_literal(update['last_run_id'])}")

    sql = f"""
    UPDATE "{DATABASE}"."table_watermarks"
    SET {', '.join(set_clauses)}
    WHERE table_id = {_bigint_literal(table_id)}
    """
    _run_statement(sql)


# Column -> literal-rendering function for pipeline_runs' schema (BIGINT/
# BIGINT/STRING/TIMESTAMP/TIMESTAMP/STRING/BIGINT/STRING, per
# 01_metadata_setup.py) - anything not listed here falls back to
# _sql_literal()'s plain string/NULL rendering.
_PIPELINE_RUNS_LITERAL_FN = {
    "run_id": _bigint_literal,
    "table_id": _bigint_literal,
    "start_time": _timestamp_literal,
    "end_time": _timestamp_literal,
    "number_of_records": _bigint_literal,
}


def _insert_pipeline_run(run: dict) -> None:
    """
    run = {"run_id", "table_id", "layer", "start_time", "end_time", "status",
           "number_of_records", "error_message"} - matches pipeline_runs'
    schema from 01_metadata_setup.py. Any missing key is inserted as NULL.
    """
    columns = ["run_id", "table_id", "layer", "start_time", "end_time", "status",
               "number_of_records", "error_message"]
    values = ", ".join(
        _PIPELINE_RUNS_LITERAL_FN.get(c, _sql_literal)(run.get(c))
        for c in columns
    )
    sql = f"""
    INSERT INTO "{DATABASE}"."pipeline_runs" ({', '.join(columns)})
    VALUES ({values})
    """
    _run_statement(sql)


def _apply_updates(event: dict) -> list:
    """Applies every update in the event, collecting (not raising on) errors
    so a partial failure doesn't stop the rest - same "collect and report"
    approach as _read_all_tables()."""
    update_errors = []

    for wm in event.get("watermark_updates", []) or []:
        try:
            _update_watermark(wm)
        except Exception as exc:  # noqa: BLE001
            update_errors.append({"watermark_update": wm, "error": str(exc)})

    run = event.get("pipeline_run")
    if run:
        try:
            _insert_pipeline_run(run)
        except Exception as exc:  # noqa: BLE001
            update_errors.append({"pipeline_run": run, "error": str(exc)})

    return update_errors


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def lambda_handler(event, context):
    event = event or {}
    action = event.get("action", "read")

    update_errors = _apply_updates(event) if action == "update" else []

    # Both actions return the SAME shape - the calling notebook cell doesn't
    # need to know or care which action it sent, it just reads ["tables"].
    tables, read_errors = _read_all_tables()

    return {
        "action": action,
        "database": DATABASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": tables,
        "errors": read_errors,
        "update_errors": update_errors,
    }


if __name__ == "__main__":
    # Local smoke test: `python lambda_function.py` (needs the same env vars
    # + AWS credentials set locally that setup.sh gives the deployed Lambda).
    print(json.dumps(lambda_handler({"action": "read"}, None), indent=2, default=str))
