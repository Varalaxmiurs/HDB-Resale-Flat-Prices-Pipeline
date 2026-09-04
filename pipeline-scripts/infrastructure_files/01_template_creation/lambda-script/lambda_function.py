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

    # bump a watermark + log ONE run row, get the updated state back
    resp = lambda_client.invoke(
        FunctionName="hdb-eventdriven-metadata-reader",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "action": "update",
            "watermark_updates": [
                {"table_id": 1, "last_watermark_value": "2026-08-23T00:00:00", "last_run_id": 101},
            ],
            "pipeline_run": {
                "run_id": 101, "table_id": 1, "layer": "job_3_cleaned_iceberg",
                "start_time": "2026-08-23T00:50:00", "end_time": "2026-08-23T00:53:00",
                "status": "SUCCEEDED", "number_of_records": 3, "error_message": None,
            },
        }).encode(),
    )
    metadata = json.loads(resp["Payload"].read())
    # metadata["tables"] is now the metadata state a following cell can act on

    # OR: log every layer's row for this run in one call, sourced straight
    # from audit_iceberg's own real rows_out/run_timestamp (used by the
    # state machine's success-path WriteContext step - see
    # build_state_machine_definition.py) instead of assembling them by hand:
    resp = lambda_client.invoke(
        FunctionName="hdb-eventdriven-metadata-reader",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "action": "update",
            "sync_pipeline_runs_from_audit": {
                "run_id": 101, "since": "2026-08-23T00:45:00", "table_id": 1,
            },
        }).encode(),
    )

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

import html as html_lib
import json
import os
import random
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
QUERY_TIMEOUT_SECONDS = 45

athena = boto3.client("athena", region_name=REGION)

STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
SES_SENDER_EMAIL = os.environ.get("SES_SENDER_EMAIL", "").strip()
SES_RECIPIENT_EMAILS = [a.strip() for a in os.environ.get("SES_RECIPIENT_EMAILS", "").split(",") if a.strip()]
USE_SES_FOR_ALERTS = bool(SES_SENDER_EMAIL) and bool(SES_RECIPIENT_EMAILS)

STATE_FAILURE_INFO = {
    "IngestToRawLayer":        {"layer": "raw_iceberg",         "job_name": "hdb-job-2-raw-iceberg"},
    "ProfileRawData":          {"layer": "data_profiling",      "job_name": "hdb-job-2b-data-profiling"},
    "CleanDataLayer":          {"layer": "cleaned_iceberg",     "job_name": "hdb-job-3-cleaned-iceberg"},
    "TransformDataLayer":      {"layer": "transformed_iceberg", "job_name": "hdb-job-4-transformed-iceberg"},
    "HashAndVersionDataLayer": {"layer": "hashed_iceberg",      "job_name": "hdb-job-5-hashed-iceberg"},
    "CompactIcebergTables":    {"layer": "compact_metadata",    "job_name": "hdb-job-0-compact-metadata"},
}

sfn_client = boto3.client("stepfunctions", region_name=REGION)
sns_client = boto3.client("sns", region_name=REGION)
sesv2_client = boto3.client("sesv2", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)


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
            columns = [c.get("VarCharValue") for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            values = [c.get("VarCharValue") for c in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


def _sql_literal(value) -> str:
    """Render a Python value as an Athena SQL literal (quoted string,
    CAST(NULL AS VARCHAR), or bare number).

    BUGFIX: a bare NULL (no type at all) can raise TYPE_MISMATCH against
    an already-typed target column - Athena/Trino's own VALUES-clause type
    check can report a plain NULL literal as "unknown" rather than
    implicitly treating it as assignable to whatever the target column is
    (see the identical, confirmed-in-production case this sidesteps for
    BIGINT/TIMESTAMP columns in _bigint_literal()/_timestamp_literal()
    below - error_message/layer/status are VARCHAR here, which is the
    Athena type least likely to hit this, but casting explicitly is free
    and removes the ambiguity entirely rather than relying on that)."""
    if value is None:
        return "CAST(NULL AS VARCHAR)"
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
    sidesteps Athena's own type-inference instead of relying on it.

    BUGFIX (confirmed in production): a bare NULL used to be returned
    as-is here for a missing value - e.g. number_of_records=None, which
    _handle_execution_failure() passes on EVERY failed run, since a
    failure means no output count was ever produced. An untyped NULL
    against pipeline_runs' BIGINT number_of_records column raised the
    exact same TYPE_MISMATCH ("unknown" vs "bigint") that job_3 hit
    against failed_iceberg - silently swallowed by _insert_pipeline_run's
    caller's try/except, so the FAILED row for that run was simply never
    written. CAST(NULL AS BIGINT) pins the literal's type so Athena
    accepts it as a legitimate BIGINT NULL instead."""
    return f"CAST({_sql_literal(value)} AS BIGINT)"


def _timestamp_literal(value) -> str:
    """A bare quoted ISO datetime string (e.g. '2026-08-24T06:57:18.887661')
    infers as VARCHAR, not TIMESTAMP - same INSERT-strictness problem as
    _bigint_literal() above, for pipeline_runs.start_time/end_time (both
    TIMESTAMP). Athena's TIMESTAMP literal syntax wants a space instead of
    the ISO 'T' separator and no trailing 'Z' - both stripped here.

    BUGFIX: same untyped-NULL issue as _bigint_literal() - a missing/None
    value (e.g. an execution event with no stopDate yet) used to return a
    bare NULL instead of CAST(NULL AS TIMESTAMP), which can TYPE_MISMATCH
    against pipeline_runs' TIMESTAMP columns the exact same way."""
    if value is None:
        return "CAST(NULL AS TIMESTAMP)"
    normalized = str(value).replace("T", " ")
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    return f"TIMESTAMP {_sql_literal(normalized)}"


def _read_all_tables() -> tuple:
    tables = {}
    errors = {}
    for table in METADATA_TABLES:
        try:
            query_id = _run_statement(f'SELECT * FROM "{DATABASE}"."{table}"')
            tables[table] = _fetch_rows_as_dicts(query_id)
        except Exception as exc:
            errors[table] = str(exc)
    return tables, errors


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


_PIPELINE_RUNS_LITERAL_FN = {
    "run_id": _bigint_literal,
    "table_id": _bigint_literal,
    "start_time": _timestamp_literal,
    "end_time": _timestamp_literal,
    "number_of_records": _bigint_literal,
}


def _clean_error_message(raw):
    """Best-effort extraction of just the human-readable message from a
    Step Functions Cause string. A Glue/task failure formats Cause as a
    JSON blob (AllocatedCapacity/Attempt/CompletedOn/... envelope noise
    around the one field that actually matters, ErrorMessage); other
    failures may leave Cause as plain text already. Falls back to the raw
    value completely unparsed on ANY failure to parse - this sits on the
    failure-alert path, so it must never itself raise or hide the real
    error behind a parsing bug.
    """
    if not raw or not isinstance(raw, str):
        return raw
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    if isinstance(parsed, dict) and parsed.get("ErrorMessage"):
        return parsed["ErrorMessage"]
    return raw


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


_SYNCED_JOB_NAMES = (
    "job_2_raw_iceberg", "job_2b_data_profiling", "job_3_cleaned_iceberg",
    "job_4_transformed_iceberg", "job_5_hashed_iceberg",
)

_FAILED_STAGE_BY_JOB_NAME = {
    "job_2_raw_iceberg": "raw",
    "job_3_cleaned_iceberg": "cleaned",
    "job_4_transformed_iceberg": "transformed",
    "job_5_hashed_iceberg": "hashed",
}
_JOB_NAME_BY_FAILED_STAGE = {tag: job_name for job_name, tag in _FAILED_STAGE_BY_JOB_NAME.items()}


def _format_run_summary_table(rows: list, reject_rows: list) -> str:
    """Plain fixed-width text tables (SNS email is plain text, no HTML/SES
    templating in this build). TWO tables, per the user's explicit ask
    that ALL of this - not just totals - show up in the email itself:

      1. one line per layer: LAYER / STATUS / RECORDS (kept) / REJECTED /
         DURATION(s) / DATE - RECORDS+REJECTED is the same reconcile_count()
         math each job already checks on itself (my_count + my_rejected_count
         == expected_count from the previous layer); DURATION(s) is the real
         per-job elapsed time record_audit() computed from that job's own
         start_time, "-" for any row synced before duration_seconds existed.
      2. one line per (layer, reason) - the reject-reason breakdown a
         person would otherwise have to go query failed_iceberg for by
         hand (see _fetch_reject_rows()).

    A "Total run duration" line follows the per-layer table, summing only
    the rows that actually have a duration_seconds value.

    reject_rows is a flat list of (job_name, reason, count) triples, kept
    ungrouped by the caller so this function stays the only place that
    decides how the report is laid out."""
    if not rows:
        return "(no layers recorded for this run)"

    reject_totals = {}
    for job_name, _reason, count in reject_rows:
        reject_totals[job_name] = reject_totals.get(job_name, 0) + count

    header = f"{'LAYER':<28}{'STATUS':<11}{'RECORDS':>9}{'REJECTED':>9}{'DURATION(s)':>12}   DATE"
    lines = [header, "-" * len(header)]
    durations = []
    for row in sorted(rows, key=lambda r: r["job_name"]):
        date_str = str(row["run_timestamp"]).replace("T", " ")[:16]
        rejected = reject_totals.get(row["job_name"], 0)
        duration = row.get("duration_seconds")
        if duration is not None:
            duration = float(duration)
            durations.append(duration)
        duration_str = f"{duration:.1f}" if duration is not None else "-"
        lines.append(
            f"{row['job_name']:<28}{'SUCCEEDED':<11}{str(row['rows_out']):>9}"
            f"{str(rejected):>9}{duration_str:>12}   {date_str}"
        )

    if durations:
        lines += ["", f"Total run duration: {sum(durations):.1f}s across {len(durations)} layer(s)"]

    if reject_rows:
        reason_header = f"{'LAYER':<28}{'REASON':<32}{'COUNT':>8}"
        lines += ["", "Reject reason breakdown:", reason_header, "-" * len(reason_header)]
        for job_name, reason, count in sorted(reject_rows, key=lambda r: (r[0], -r[2])):
            lines.append(f"{job_name:<28}{reason:<32}{count:>8}")

    return "\n".join(lines)


def _format_run_summary_markdown(rows: list, reject_rows: list) -> str:
    """Same data as _format_run_summary_table(), as a self-contained
    Markdown document (real tables, not fixed-width text) - this is what
    gets saved as alert-logs/success/<execution>.md and referenced
    DIRECTLY by build_state_machine_definition.py's SaveSuccessLog state
    ($.write_context_result.Payload.run_summary_markdown, no
    States.Format() wrapping). Keeping the whole multi-line document
    assembly here in Python, rather than as a static template embedded in
    the ASL JSON, means every newline in it is just a normal Python string
    - no risk from routing templated multi-line text through ASL's own
    intrinsic-function string-literal parsing."""
    lines = [
        "# HDB Resale Flat Prices Pipeline - Run Summary",
        "",
        "**Status:** SUCCESS - all 6 steps ran cleanly (ingestion, raw, "
        "data profiling, cleaned, transformed, hashed).",
        "",
        "## Run Summary",
        "",
    ]
    if not rows:
        lines.append("_(no layers recorded for this run)_")
    else:
        reject_totals = {}
        for job_name, _reason, count in reject_rows:
            reject_totals[job_name] = reject_totals.get(job_name, 0) + count
        lines.append("| Layer | Status | Records | Rejected | Duration (s) | Date |")
        lines.append("|---|---|---|---|---|---|")
        durations = []
        for row in sorted(rows, key=lambda r: r["job_name"]):
            date_str = str(row["run_timestamp"])[:19]
            rejected = reject_totals.get(row["job_name"], 0)
            duration = row.get("duration_seconds")
            if duration is not None:
                duration = float(duration)
                durations.append(duration)
            duration_str = f"{duration:.1f}" if duration is not None else "-"
            lines.append(f"| {row['job_name']} | SUCCEEDED | {row['rows_out']} | {rejected} | {duration_str} | {date_str} |")
        if durations:
            lines += ["", f"**Total run duration:** {sum(durations):.1f}s across {len(durations)} layer(s)"]

    if reject_rows:
        lines += ["", "## Reject Reason Breakdown", "", "| Layer | Reason | Count |", "|---|---|---|"]
        for job_name, reason, count in sorted(reject_rows, key=lambda r: (r[0], -r[2])):
            lines.append(f"| {job_name} | {reason} | {count} |")

    lines += [
        "",
        "_Metadata was read before this run and read again after a real write to "
        "pipeline_runs (via sync_pipeline_runs_from_audit) - the table above reflects "
        "genuinely new state, not just two identical reads._",
    ]
    return "\n".join(lines)


def _format_run_summary_html(rows: list, reject_rows: list) -> str:
    """HTML version of the same data _format_run_summary_table()/
    _format_run_summary_markdown() already build - four stat tiles (steps
    succeeded, rows ingested, rows in hashed_iceberg, total rejected) plus
    two real <table> elements (Run summary, Reject reason breakdown), all
    with inline styles - email clients strip <style> blocks and ignore CSS
    variables/media queries too unreliably to use them here, so this
    fragment is fully self-contained. build_state_machine_definition.py's
    success_html drops this straight into the email body via a single
    States.Format() placeholder, the same way it used to drop in
    run_summary_table's plain-text block.

    Using real <table> cells (instead of _format_run_summary_table()'s
    space-padded fixed-width text inside a <pre>) is also what actually
    fixes the "DATE wraps onto its own line" report: a <pre> table's
    columns are just whitespace, so a client that reflows a long line
    tears the row apart mid-column; a real <table> cell just wraps its own
    content without disturbing the columns next to it.
    """
    if not rows:
        return '<p style="font-size:13px;color:#5b6472;margin:0;">(no layers recorded for this run)</p>'

    reject_totals = {}
    for job_name, _reason, count in reject_rows:
        reject_totals[job_name] = reject_totals.get(job_name, 0) + count

    sorted_rows = sorted(rows, key=lambda r: r["job_name"])
    by_job_name = {r["job_name"]: r for r in sorted_rows}

    steps_succeeded = len({r["job_name"] for r in sorted_rows})
    total_steps = len(_SYNCED_JOB_NAMES)
    rows_ingested = by_job_name.get("job_2_raw_iceberg", {}).get("rows_out", "-")
    rows_hashed = by_job_name.get("job_5_hashed_iceberg", {}).get("rows_out", "-")
    total_rejected = sum(reject_totals.values())

    def _stat_tile(label, value, color="#1a2233"):
        return (
            '<td style="padding:0 6px;" width="25%">'
            '<div style="background:#ffffff;border:1px solid #dde2e8;border-radius:8px;padding:12px 14px;">'
            f'<div style="font-size:10.5px;color:#5b6472;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">{label}</div>'
            f'<div style="font-family:{MONO_FONT};font-size:19px;font-weight:700;color:{color};">{value}</div>'
            '</div></td>'
        )

    stat_row = (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:16px;"><tr>'
        + _stat_tile("Steps succeeded", f"{steps_succeeded}/{total_steps}", "#0e7c7b")
        + _stat_tile("Rows ingested", rows_ingested)
        + _stat_tile("Rows in hashed_iceberg", rows_hashed, "#0e7c7b")
        + _stat_tile("Total rejected", total_rejected, "#b7791f" if total_rejected else "#1a2233")
        + '</tr></table>'
    )

    row_cells = []
    durations = []
    for row in sorted_rows:
        date_str = str(row["run_timestamp"]).replace("T", " ")[:16]
        rejected = reject_totals.get(row["job_name"], 0)
        duration = row.get("duration_seconds")
        if duration is not None:
            duration = float(duration)
            durations.append(duration)
        duration_str = f"{duration:.1f}" if duration is not None else "-"
        rejected_style = "color:#b7791f;font-weight:700;" if rejected else "color:#5b6472;"
        row_cells.append(
            "<tr>"
            f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;font-family:{MONO_FONT};font-size:12px;">{row["job_name"]}</td>'
            '<td style="padding:8px 10px;border-top:1px solid #dde2e8;">'
            '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#e6f6ee;color:#1a8f5a;font-size:11px;font-weight:700;">SUCCEEDED</span>'
            '</td>'
            f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;text-align:right;font-family:{MONO_FONT};font-size:12px;">{row["rows_out"]}</td>'
            f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;text-align:right;font-family:{MONO_FONT};font-size:12px;{rejected_style}">{rejected}</td>'
            f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;text-align:right;font-family:{MONO_FONT};font-size:12px;">{duration_str}</td>'
            f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;font-family:{MONO_FONT};font-size:11.5px;color:#5b6472;">{date_str}</td>'
            "</tr>"
        )

    duration_note = ""
    if durations:
        duration_note = (
            '<div style="font-size:12px;color:#5b6472;margin:10px 2px 0;">Total run duration: '
            f'<strong style="color:#1a2233;">{sum(durations):.1f}s</strong> across {len(durations)} layer(s)</div>'
        )

    summary_table = (
        '<div style="background:#ffffff;border:1px solid #dde2e8;border-radius:8px;padding:16px 18px;margin-bottom:16px;">'
        '<div style="font-size:13px;font-weight:700;margin-bottom:2px;color:#1a2233;">Run summary</div>'
        '<div style="font-size:12px;color:#5b6472;margin-bottom:10px;">One row per layer, sourced from audit_iceberg and written to pipeline_runs.</div>'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:12px;border-collapse:collapse;">'
        '<tr style="background:#eef1f4;">'
        '<th align="left" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Layer</th>'
        '<th align="left" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Status</th>'
        '<th align="right" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Records</th>'
        '<th align="right" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Rejected</th>'
        '<th align="right" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Duration(s)</th>'
        '<th align="left" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Date</th>'
        "</tr>"
        + "".join(row_cells)
        + "</table>"
        + duration_note
        + "</div>"
    )

    reject_html = ""
    if reject_rows:
        reason_cells = []
        for job_name, reason, count in sorted(reject_rows, key=lambda r: (r[0], -r[2])):
            reason_cells.append(
                "<tr>"
                f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;font-family:{MONO_FONT};font-size:12px;">{job_name}</td>'
                f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;font-family:{MONO_FONT};font-size:12px;">{reason}</td>'
                f'<td style="padding:8px 10px;border-top:1px solid #dde2e8;text-align:right;font-family:{MONO_FONT};font-size:12px;">{count}</td>'
                "</tr>"
            )
        reject_html = (
            '<div style="background:#ffffff;border:1px solid #dde2e8;border-radius:8px;padding:16px 18px;">'
            '<div style="font-size:13px;font-weight:700;margin-bottom:2px;color:#1a2233;">Reject reason breakdown</div>'
            '<div style="font-size:12px;color:#5b6472;margin-bottom:10px;">Every (layer, reason) pair failed_iceberg recorded for this run.</div>'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:12px;border-collapse:collapse;">'
            '<tr style="background:#eef1f4;">'
            '<th align="left" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Layer</th>'
            '<th align="left" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Reason</th>'
            '<th align="right" style="padding:8px 10px;font-size:10.5px;text-transform:uppercase;color:#5b6472;">Count</th>'
            "</tr>"
            + "".join(reason_cells)
            + "</table></div>"
        )

    return stat_row + summary_table + reject_html


def _format_run_summary_full_html(rows: list, reject_rows: list) -> str:
    """Complete, standalone HTML document wrapping _format_run_summary_
    html()'s fragment - built ENTIRELY here in Python, no States.Format()
    involved, for exactly the same reason _format_run_summary_markdown()
    already is: a States.Format() intrinsic-function result behaves fine
    when handed to SES's sendEmail Data field (a plain String in the SDK
    model), but arn:aws:states:::aws-sdk:s3:putObject's Body field is
    typed as a Blob - handing THAT field a States.Format() result gets it
    JSON-string-encoded (quotes and all) instead of written as raw text,
    which is exactly the ".html file opens as a literal quoted JSON
    string" bug this function replaces. build_state_machine_definition.py
    references this value directly via a plain $.path (like
    run_summary_markdown already does), never via States.Format().

    This is a SEPARATE value from success_html (the one the actual email
    still uses, built in build_state_machine_definition.py via
    States.Format() - that field tolerates it fine, so it is untouched).
    The two are visually near-identical by design, just assembled through
    two different mechanisms for two different SDK field types.
    """
    fragment = _format_run_summary_html(rows, reject_rows)
    return (
        '<!doctype html><html lang="en"><head><meta charset="UTF-8">'
        '<title>HDB Pipeline Run - SUCCESS</title></head>'
        '<body style="margin:0;background:#f5f7f9;font-family:Arial,Helvetica,sans-serif;color:#1a2233;">'
        '<div style="max-width:640px;margin:0 auto;padding:32px 20px;">'
        f'<div style="font-family:{MONO_FONT};font-size:12px;letter-spacing:0.05em;text-transform:uppercase;color:#5b6472;margin-bottom:8px;">HDB Pipeline Run - SUCCESS</div>'
        '<h1 style="font-size:22px;font-weight:700;margin:0 0 6px;color:#1a2233;">Resale Flat Prices ETL - run completed</h1>'
        '<p style="color:#5b6472;font-size:14px;margin:0 0 20px;">All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, transformed, hashed.</p>'
        + fragment +
        '<div style="font-size:12px;color:#5b6472;text-align:center;padding-top:6px;">Metadata was read before this run and read again after a real write to pipeline_runs, so the report above reflects genuinely new state, not two identical reads.</div>'
        '</div></body></html>'
    )


def _fetch_reject_rows(since_literal: str) -> list:
    """Every (job_name, reason, count) triple failed_iceberg recorded for
    this run - the real reject-reason breakdown behind the REJECTED column
    above, sourced the same way _sync_pipeline_runs_from_audit() sources
    RECORDS: read what the jobs already wrote for real via route_to_failed()
    (common.py), don't recompute or guess it here.

    Returns [] (not raises) on any query error - a broken breakdown query
    should degrade the email to REJECTED=0 / no breakdown table, not blank
    out the whole run-summary table the way the pre-CAST() audit_iceberg
    bug once did (see _sync_pipeline_runs_from_audit()'s BUGFIX comment)."""
    stage_tags_sql = ", ".join(_sql_literal(tag) for tag in _JOB_NAME_BY_FAILED_STAGE)
    try:
        query_id = _run_statement(
            f'SELECT _failed_stage, _failure_reason, COUNT(*) AS cnt FROM "{DATABASE}"."failed_iceberg" '
            f"WHERE CAST(REPLACE(_failed_at, 'T', ' ') AS TIMESTAMP) >= {since_literal} "
            f"AND _failed_stage IN ({stage_tags_sql}) "
            f"GROUP BY _failed_stage, _failure_reason"
        )
        rows = _fetch_rows_as_dicts(query_id)
    except Exception:
        return []
    return [
        (_JOB_NAME_BY_FAILED_STAGE.get(row["_failed_stage"], row["_failed_stage"]),
         row["_failure_reason"], int(row["cnt"]))
        for row in rows
    ]


def _persist_run_report(execution_name: str, run_summary_markdown: str, run_summary_full_html: str) -> None:
    """Writes both the markdown and HTML success reports directly to S3
    as raw UTF-8 bytes, via boto3 - called from THIS Lambda, never from a
    Step Functions aws-sdk:s3:putObject Task.

    BUGFIX (confirmed in production, twice): aws-sdk:s3:putObject's Body
    parameter is Blob-typed in the S3 SDK model. Feeding it a dynamic
    value via ANY '.$' JSONPath reference - not just a States.Format()
    intrinsic, a BARE path reference has the exact same problem - makes
    Step Functions JSON-encode the string (wrapping it in quotes,
    escaping every internal quote) instead of writing it as raw bytes.
    The first fix attempt (swapping States.Format() for a bare '$.path'
    reference) was based on the mistaken assumption that States.Format()
    itself was the cause; a real production run afterwards proved the
    escaping persisted regardless, because the actual cause is Step
    Functions' own Blob-marshalling of ANY dynamically-resolved value,
    not the specific intrinsic function used to resolve it.

    The only reliable fix is to never let Step Functions' ASL layer touch
    the Blob field at all: have this Lambda call s3_client.put_object()
    directly with the string encoded to real bytes itself, the same way
    every other file in this codebase already writes to S3. This is also
    why the SES email body was NEVER affected by this bug even before
    this fix - sesv2:sendEmail's Content.Simple.Body.Html.Data is a plain
    String-typed field, not Blob, so States.Format()/'.$' references
    there were always safe."""
    if run_summary_markdown:
        s3_client.put_object(
            Bucket=AUDIT_BUCKET,
            Key=f"alert-logs/success/{execution_name}.md",
            Body=run_summary_markdown.encode("utf-8"),
            ContentType="text/markdown; charset=utf-8",
        )
    if run_summary_full_html:
        s3_client.put_object(
            Bucket=AUDIT_BUCKET,
            Key=f"alert-logs/success/{execution_name}.html",
            Body=run_summary_full_html.encode("utf-8"),
            ContentType="text/html; charset=utf-8",
        )


def _sync_pipeline_runs_from_audit(run_id, since_iso: str, table_id: int = 1, execution_name: str = None) -> tuple:
    """
    Reads THIS run's own rows out of audit_iceberg (already written for
    real by every job_*.py's record_audit() call - see common.py) and turns
    each into a pipeline_runs row: layer=job_name, number_of_records=
    rows_out, start_time/end_time=run_timestamp. This is what makes
    "count of each layer and date" real data instead of something invented
    here - audit_iceberg is the source of truth, this just re-shapes it
    into the metadata schema's pipeline_runs table.

    duration_seconds also comes straight from audit_iceberg (record_audit()
    computes it from the start_time each job_*.py already captures at the
    top of main()) and rides along in `rows` purely for the two formatter
    functions below - it isn't part of pipeline_runs' own schema, so
    _insert_pipeline_run() doesn't touch it. Rows written before this
    column existed read back as NULL/None here (schema_evolution=True on
    audit_iceberg), which the formatters already render as "-".

    A row only exists in audit_iceberg AFTER a job's real write already
    succeeded (record_audit() runs near the end of main()), so presence
    here always means status=SUCCEEDED. A job that crashed before reaching
    record_audit() has NO row here at all - that's expected, not a bug:
    failures are written directly from the state machine's own Catch
    branches (via the plain "pipeline_run" single-insert path above),
    which have the actual error info audit_iceberg never gets to record.
    """
    since_literal = _timestamp_literal(since_iso)
    job_names_sql = ", ".join(_sql_literal(j) for j in _SYNCED_JOB_NAMES)
    query_id = _run_statement(
        f'SELECT job_name, rows_out, run_timestamp, duration_seconds FROM "{DATABASE}"."audit_iceberg" '
        f"WHERE CAST(REPLACE(run_timestamp, 'T', ' ') AS TIMESTAMP) >= {since_literal} "
        f"AND job_name IN ({job_names_sql})"
    )
    rows = _fetch_rows_as_dicts(query_id)
    for row in rows:
        _insert_pipeline_run({
            "run_id": run_id,
            "table_id": table_id,
            "layer": row["job_name"],
            "start_time": row["run_timestamp"],
            "end_time": row["run_timestamp"],
            "status": "SUCCEEDED",
            "number_of_records": row["rows_out"],
            "error_message": None,
        })
    reject_rows = _fetch_reject_rows(since_literal)
    run_summary_table = _format_run_summary_table(rows, reject_rows)
    run_summary_markdown = _format_run_summary_markdown(rows, reject_rows)
    run_summary_html = _format_run_summary_html(rows, reject_rows)
    run_summary_full_html = _format_run_summary_full_html(rows, reject_rows)

    if execution_name:
        # Fail-soft, same reasoning as record_audit() in common.py: a
        # persist failure here is a logging nicety, not the pipeline's
        # actual output - it must never prevent the run summary from
        # being returned (and therefore emailed).
        try:
            _persist_run_report(execution_name, run_summary_markdown, run_summary_full_html)
        except Exception as exc:
            print(f"_persist_run_report failed (non-fatal): {exc}")

    return (run_summary_table, run_summary_markdown, run_summary_html, run_summary_full_html)


def _apply_updates(event: dict) -> tuple:
    """Applies every update in the event, collecting (not raising on) errors
    so a partial failure doesn't stop the rest - same "collect and report"
    approach as _read_all_tables(). Returns (update_errors, run_summary_table,
    run_summary_markdown, run_summary_html, clean_error_message) -
    run_summary_table/run_summary_markdown/run_summary_html are only
    populated when a sync_pipeline_runs_from_audit call happened, so the
    email/persisted log have real content to show."""
    update_errors = []
    run_summary_table = ""
    run_summary_markdown = ""
    run_summary_html = ""
    run_summary_full_html = ""

    for wm in event.get("watermark_updates", []) or []:
        try:
            _update_watermark(wm)
        except Exception as exc:
            update_errors.append({"watermark_update": wm, "error": str(exc)})

    clean_error_message = None
    run = event.get("pipeline_run")
    if run:
        clean_error_message = _clean_error_message(run.get("error_message"))
        run = {**run, "error_message": clean_error_message}
        try:
            _insert_pipeline_run(run)
        except Exception as exc:
            update_errors.append({"pipeline_run": run, "error": str(exc)})

    for run in event.get("pipeline_runs", []) or []:
        try:
            _insert_pipeline_run(run)
        except Exception as exc:
            update_errors.append({"pipeline_run": run, "error": str(exc)})

    sync = event.get("sync_pipeline_runs_from_audit")
    if sync:
        try:
            run_summary_table, run_summary_markdown, run_summary_html, run_summary_full_html = _sync_pipeline_runs_from_audit(
                run_id=sync["run_id"], since_iso=sync["since"], table_id=sync.get("table_id", 1),
                execution_name=sync.get("execution_name"),
            )
        except Exception as exc:
            update_errors.append({"sync_pipeline_runs_from_audit": sync, "error": str(exc)})

    return update_errors, run_summary_table, run_summary_markdown, run_summary_html, run_summary_full_html, clean_error_message


CONSOLE_EXECUTION_URL_FMT = (
    "https://{region}.console.aws.amazon.com/states/home?region={region}"
    "#/v2/executions/details/{execution_arn}"
)

MONO_FONT = "Consolas,Menlo,monospace"


def _epoch_ms_to_iso(value):
    """EventBridge's own 'Step Functions Execution Status Change' event
    detail carries startDate/stopDate as raw epoch-milliseconds numbers
    (NOT ISO strings, unlike every other timestamp in this file) - convert
    to an ISO string _timestamp_literal() already knows how to render.
    A value that's already a string is passed through untouched (defensive
    - covers a future/alternate event shape without fighting it)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    return str(value)


def _find_failed_state(execution_arn: str):
    """Walks this execution's own history (GetExecutionHistory's default
    oldest-first order), tracking whichever state was most recently
    entered, and returns (state_name, raw_cause_or_error) for the LAST
    *FailedEventDetails event found - "last" rather than "first" only
    matters if a Task had its own Retry (none of ours do today, but costs
    nothing to be correct if that changes later). Returns (None, None) if
    history has no *Failed event at all (defensive - shouldn't happen,
    since Step 11F only wires FAILED executions to this Lambda)."""
    current_state_name = None
    last_failure = (None, None)
    next_token = None
    while True:
        kwargs = {"executionArn": execution_arn, "maxResults": 1000}
        if next_token:
            kwargs["nextToken"] = next_token
        page = sfn_client.get_execution_history(**kwargs)
        for evt in page.get("events", []):
            for key, val in evt.items():
                if not isinstance(val, dict):
                    continue
                if key == "stateEnteredEventDetails":
                    current_state_name = val.get("name", current_state_name)
                elif key.endswith("FailedEventDetails"):
                    cause = val.get("cause") or val.get("error") or "Unknown error"
                    last_failure = (current_state_name, cause)
        next_token = page.get("nextToken")
        if not next_token:
            break
    return last_failure


def _build_failure_alert(execution_name, execution_arn, state_name, layer,
                          job_name, clean_message, start_iso, stop_iso):
    """Plain Python f-strings - no States.Format() escaping concerns at all
    (see build_state_machine_definition.py's module docstring for why that
    mattered when this lived inline in the state machine). clean_message
    is HTML-escaped only for the html_body below - text_body and the
    pipeline_runs row both keep it verbatim."""
    console_url = CONSOLE_EXECUTION_URL_FMT.format(region=REGION, execution_arn=execution_arn)
    step_label = state_name or "(unknown state)"
    job_label = job_name or "(not a Glue job step)"
    subject = "HDB Pipeline Run - FAILURE"

    text_body = (
        "HDB Resale Flat Prices Pipeline - ETL process FAILED\n"
        "======================================================\n"
        f"Execution:  {execution_name}\n"
        f"Failed at:  {step_label}\n"
        f"Glue job:   {job_label}\n"
        f"Layer:      {layer}\n"
        f"Started:    {start_iso}\n"
        f"Stopped:    {stop_iso}\n\n"
        f"Error:\n{clean_message}\n\n"
        f"Console:    {console_url}\n\n"
        "This execution stopped uncaught at the real failing step, so once "
        "the underlying problem is fixed it can be resumed from exactly "
        "here with Step Functions' own Redrive action (console: Actions -> "
        "Redrive execution, or `aws stepfunctions redrive-execution "
        "--execution-arn ...`) - already-succeeded steps will not re-run."
    )

    log_markdown = (
        "# HDB Resale Flat Prices Pipeline - Run FAILED\n\n"
        "| | |\n|---|---|\n"
        f"| **Execution** | {execution_name} |\n"
        f"| **Failed at** | {step_label} |\n"
        f"| **Glue job** | {job_label} |\n"
        f"| **Layer** | {layer} |\n"
        f"| **Started** | {start_iso} |\n"
        f"| **Stopped** | {stop_iso} |\n\n"
        f"## Error\n\n```\n{clean_message}\n```\n\n"
        f"[Open execution in console]({console_url})\n\n"
        "This execution stopped uncaught at the real failing step, so once "
        "the underlying problem is fixed it can be resumed from exactly "
        "here with Step Functions' own Redrive action (console: Actions -> "
        "Redrive execution, or `aws stepfunctions redrive-execution "
        "--execution-arn ...`) - already-succeeded steps will not re-run."
    )

    esc_msg = html_lib.escape(str(clean_message))
    esc_exec = html_lib.escape(str(execution_name))
    esc_step = html_lib.escape(str(step_label))
    esc_job = html_lib.escape(str(job_label))
    esc_layer = html_lib.escape(str(layer))

    html_body = (
        '<!doctype html><html lang="en"><head><meta charset="UTF-8"></head>'
        '<body style="margin:0;background:#f5f7f9;font-family:Arial,Helvetica,sans-serif;color:#1a2233;">'
        '<div style="max-width:640px;margin:0 auto;padding:32px 20px;">'
        f'<div style="font-family:{MONO_FONT};font-size:12px;letter-spacing:0.05em;text-transform:uppercase;color:#b3261e;margin-bottom:8px;">HDB Pipeline Run - FAILURE</div>'
        '<h1 style="font-size:22px;font-weight:700;margin:0 0 6px;color:#1a2233;">Resale Flat Prices ETL - run failed</h1>'
        f'<p style="color:#5b6472;font-size:14px;margin:0 0 20px;">Execution <strong>{esc_exec}</strong> stopped at <strong>{esc_step}</strong>.</p>'
        '<div style="background:#ffffff;border:1px solid #dde2e8;border-radius:8px;padding:16px 18px;margin-bottom:16px;">'
        '<table style="width:100%;font-size:13px;color:#1a2233;border-collapse:collapse;">'
        f'<tr><td style="padding:3px 0;color:#5b6472;width:110px;">Failed at</td><td style="padding:3px 0;">{esc_step}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#5b6472;">Glue job</td><td style="padding:3px 0;">{esc_job}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#5b6472;">Layer</td><td style="padding:3px 0;">{esc_layer}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#5b6472;">Started</td><td style="padding:3px 0;">{start_iso}</td></tr>'
        f'<tr><td style="padding:3px 0;color:#5b6472;">Stopped</td><td style="padding:3px 0;">{stop_iso}</td></tr>'
        '</table></div>'
        '<div style="background:#fdecea;border:1px solid #f3c1bb;border-radius:8px;padding:14px 16px;margin-bottom:16px;">'
        '<div style="font-size:13px;font-weight:700;margin-bottom:6px;color:#b3261e;">Error</div>'
        f'<pre style="background:#ffffff;border:1px solid #f3c1bb;border-radius:6px;padding:12px 14px;font-family:{MONO_FONT};font-size:12px;line-height:1.5;color:#b3261e;white-space:pre-wrap;word-break:break-word;margin:0;overflow-x:auto;">{esc_msg}</pre>'
        '</div>'
        f'<a href="{console_url}" style="display:inline-block;background:#1a2233;color:#ffffff;text-decoration:none;font-size:13px;font-weight:700;padding:10px 16px;border-radius:6px;margin-bottom:14px;">Open execution in console</a>'
        '<div style="font-size:12px;color:#5b6472;text-align:center;padding-top:6px;">This execution stopped uncaught at the real failing step, so once the underlying problem is fixed it can be resumed from exactly here with Step Functions&rsquo; own Redrive action - already-succeeded steps will not re-run.</div>'
        '</div></body></html>'
    )
    return subject, text_body, html_body, log_markdown


def _send_failure_alert(subject, text_body, html_body):
    """Same SES-vs-SNS branch every other Notify state in this pipeline
    uses, just as a plain boto3 call instead of an ASL Parameters block."""
    if USE_SES_FOR_ALERTS:
        sesv2_client.send_email(
            FromEmailAddress=SES_SENDER_EMAIL,
            Destination={"ToAddresses": SES_RECIPIENT_EMAILS},
            Content={"Simple": {
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": html_body}, "Text": {"Data": text_body}},
            }},
        )
    else:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=text_body)


def _save_alert_log(kind: str, execution_name: str, text_body: str) -> str:
    """Persisted counterpart to the alert itself: the SNS/SES alert is only
    visible to whoever is actually subscribed, so this writes the same
    content to S3 (AUDIT_BUCKET) keyed by execution name - a durable,
    independently-viewable record regardless of whether the alert itself
    is ever read. build_state_machine_definition.py's SaveSuccessLog state
    does the same thing for the success path, same alert-logs/<kind>/
    prefix. Returns the S3 key on success; a write failure raises past a
    boto3 error and is caught at each call site instead, same posture as
    _send_failure_alert())."""
    key = f"alert-logs/{kind}/{execution_name}.md"
    s3_client.put_object(Bucket=AUDIT_BUCKET, Key=key, Body=text_body.encode("utf-8"))
    return key


def _handle_execution_failure(detail: dict) -> dict:
    """Entry point for setup.sh Step 11F's EventBridge rule - `detail` is
    the "Step Functions Execution Status Change" event's own detail block
    (executionArn/name/status/startDate/stopDate/input/output). Finds the
    real failing state via GetExecutionHistory, writes the pipeline_runs
    FAILED row, sends the alert - mirrors what an inline Catch branch used
    to do, just out-of-band and a few seconds after the execution itself
    already ended (see build_state_machine_definition.py's module
    docstring for why it moved out here)."""
    execution_arn = detail.get("executionArn", "")
    execution_name = detail.get("name", execution_arn)
    start_iso = _epoch_ms_to_iso(detail.get("startDate"))
    stop_iso = _epoch_ms_to_iso(detail.get("stopDate"))

    state_name, raw_cause = _find_failed_state(execution_arn)
    info = STATE_FAILURE_INFO.get(state_name, {}) if state_name else {}
    layer = info.get("layer", state_name or "unknown")
    job_name = info.get("job_name")
    clean_message = _clean_error_message(raw_cause) or "Unknown error"

    run = {
        "run_id": random.randint(1, 999_999_999),
        "table_id": 1,
        "layer": layer,
        "start_time": start_iso,
        "end_time": stop_iso,
        "status": "FAILED",
        "number_of_records": None,
        "error_message": clean_message,
    }
    insert_error = None
    try:
        _insert_pipeline_run(run)
    except Exception as exc:
        insert_error = str(exc)

    subject, text_body, html_body, log_markdown = _build_failure_alert(
        execution_name, execution_arn, state_name, layer, job_name,
        clean_message, start_iso, stop_iso,
    )

    log_key = None
    log_error = None
    try:
        log_key = _save_alert_log("failure", execution_name, log_markdown)
    except Exception as exc:
        log_error = str(exc)

    alert_error = None
    try:
        _send_failure_alert(subject, text_body, html_body)
    except Exception as exc:
        alert_error = str(exc)

    return {
        "handled": "execution_failure",
        "execution_arn": execution_arn,
        "failed_state": state_name,
        "layer": layer,
        "pipeline_run": run,
        "log_key": log_key,
        "log_error": log_error,
        "insert_error": insert_error,
        "alert_error": alert_error,
    }


def lambda_handler(event, context):
    event = event or {}

    if (
        event.get("source") == "aws.states"
        and event.get("detail-type") == "Step Functions Execution Status Change"
    ):
        return _handle_execution_failure(event.get("detail", {}) or {})

    action = event.get("action", "read")

    update_errors, run_summary_table, run_summary_markdown, run_summary_html, run_summary_full_html, clean_error_message = (
        _apply_updates(event) if action == "update" else ([], "", "", "", "", None)
    )

    tables, read_errors = _read_all_tables()

    return {
        "action": action,
        "database": DATABASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": tables,
        "errors": read_errors,
        "update_errors": update_errors,
        "run_summary_table": run_summary_table,
        "run_summary_markdown": run_summary_markdown,
        "run_summary_html": run_summary_html,
        "run_summary_full_html": run_summary_full_html,
        "clean_error_message": clean_error_message,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({"action": "read"}, None), indent=2, default=str))
