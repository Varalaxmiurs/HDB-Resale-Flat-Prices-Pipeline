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
from awswrangler.exceptions import QueryFailed

from config import (
    ATHENA_WORKGROUP,
    AUDIT_S3_BUCKET,
    AWS_REGION,
    GLUE_DATABASE,
    NATURAL_KEY_COLUMNS,
    SNS_TOPIC_ARN_OVERRIDE,
    SNS_TOPIC_NAME,
    TABLES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# logging.basicConfig() above sets the ROOT logger to INFO, which means
# every third-party library's logger inherits INFO too, not just this
# project's own get_logger(...) loggers - that's what put lines like
# "Found credentials in shared credentials file: ~/.aws/credentials" into
# a real run's output. None of that is useful pipeline signal, just noisy
# library internals. Pin the chatty AWS SDK libraries to WARNING so only
# real warnings/errors from them still surface, while this project's own
# loggers (job_1..job_5, to_iceberg_retry, merge_iceberg, etc.) stay at INFO.
for _noisy_logger in ("boto3", "botocore", "urllib3", "s3transfer", "awswrangler"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

# On an actual AWS Glue job, boto3's default session already resolves to the
# region the job runs in, so region_name is redundant there. Running locally
# (run_pipeline.py / the notebook's local mode) is different: boto3 falls
# back to whatever `aws configure get region` / AWS_DEFAULT_REGION resolves
# to on THIS machine, which may not match AWS_REGION from config.py - the
# region setup.sh actually created the buckets/workgroup in. A mismatch
# there surfaces as Athena's InvalidRequestException "The S3 location
# provided to save your query results is invalid ... in the same region".
# Pinning every client/call to AWS_REGION explicitly removes that ambiguity
# in both environments.
_BOTO3_SESSION = boto3.Session(region_name=AWS_REGION)

# Athena needs an explicit place to stage temporary/query results - without
# this, wr.athena.to_iceberg()/start_query_execution() calls either raise
# InvalidArgumentCombination ("Either path or workgroup path must be
# specified...") or silently fall back to the AWS per-account/region default
# results bucket, which has been the trigger for the recurring "Iceberg
# cannot find the requested entity" failures throughout this project.
# to_iceberg() takes temp_path= AND s3_output= as two DISTINCT parameters
# (confirmed via inspect.signature()/getsource() against the installed
# awswrangler version) - temp_path= is what its internal _validate_args()
# checks, while s3_output= is what its internal CREATE TABLE query's own
# ResultConfiguration uses - both must be passed together. The raw boto3
# Athena calls in execute_athena_sql() use ResultConfiguration.OutputLocation
# instead. 01_metadata_setup.py already does the boto3-side equivalent; this
# was missing here, which is why record_audit() failed on a real job_1 run.
ATHENA_RESULTS_LOCATION = f"s3://{AUDIT_S3_BUCKET}/athena-results/"

# temp_path (below) is a SEPARATE prefix from ATHENA_RESULTS_LOCATION on
# purpose. to_iceberg() writes the dataframe to temp_path as Parquet, points
# a temporary external table at that prefix, then MERGEs from it into the
# real Iceberg table - which means that MERGE scans every object sitting
# under temp_path. Every plain Athena query (including to_iceberg's own
# CREATE TABLE / MERGE statements) also drops its result as a CSV file into
# ATHENA_RESULTS_LOCATION. Pointing temp_path at that same prefix meant the
# external table's scan picked up those unrelated CSV result files alongside
# the actual staged Parquet - surfacing as
# "HIVE_BAD_DATA: Malformed Parquet file. Expected magic number: PAR1 got: 00"
# on a real job_1 run (record_audit() -> write_iceberg() -> to_iceberg()).
# Keeping the two prefixes apart means query-result CSVs and Iceberg staging
# Parquet never share a directory listing.
ICEBERG_TEMP_LOCATION = f"s3://{AUDIT_S3_BUCKET}/athena-iceberg-temp/"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


_account_id_cache = None


def get_account_id() -> str:
    """Resolves the CURRENT AWS account id live via STS's GetCallerIdentity -
    the same call `aws sts get-caller-identity` makes under the hood: ask
    AWS "who am I", using whatever credentials/role this process already
    has. Never hardcoded in source, never written to a file or Parameter
    Store - so this code stays account-agnostic (it would run unmodified
    under a different AWS account) and nothing account-identifying sits in
    a public GitHub repo. sts:GetCallerIdentity needs no special IAM grant -
    it's allowed by default for any authenticated principal.

    Cached in-memory for this process only (avoids a repeat STS call on
    every send_alert()) - re-resolved fresh on every new job run, never
    persisted anywhere between runs."""
    global _account_id_cache
    if _account_id_cache is None:
        _account_id_cache = _BOTO3_SESSION.client("sts").get_caller_identity()["Account"]
    return _account_id_cache


def _iceberg_table_exists(table: str) -> bool:
    """Checks the Glue Catalog directly, same pattern 01_metadata_setup.py's
    table_exists() already uses (and for the same reason there): asking
    Athena itself (e.g. via CREATE TABLE IF NOT EXISTS) re-validates an
    EXISTING table's Iceberg metadata even when it's a no-op, and that
    re-validation path is what's intermittently raised "Iceberg cannot find
    the requested entity" elsewhere in this project. The Glue API's own
    get_table() sidesteps that entirely - it's a plain catalog lookup."""
    glue = _BOTO3_SESSION.client("glue")
    try:
        glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


# Freshly-created (and, per 01_metadata_setup.py, freshly re-validated)
# Iceberg tables have intermittently failed with "Iceberg cannot find the
# requested entity" moments after the CREATE succeeds - AWS-side Glue Data
# Catalog / Iceberg metadata propagation lag, not a real problem with the
# table. 01_metadata_setup.py works around this with a time.sleep() between
# consecutive CREATE TABLE statements; to_iceberg() doesn't expose a hook to
# insert that pause internally, so this wraps the whole call in a small
# retry instead - same fix, applied from the outside.
ICEBERG_PROPAGATION_RETRIES = 3
ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS = 8


def _to_iceberg_with_retry(**kwargs) -> None:
    """
    IMPORTANT - retrying to_iceberg() is NOT automatically safe for EITHER
    mode="append" OR mode="overwrite": the "cannot find the requested
    entity" propagation-lag error can surface from a step INSIDE
    to_iceberg() that runs AFTER the actual data write already committed
    (e.g. its own post-write catalog check) - not just from the CREATE
    TABLE step. A naive blind retry then re-runs the WHOLE call, repeating
    whatever write already landed. This has now bitten BOTH modes on real
    runs:
      - mode="append": raw_iceberg ended up with 1,377,021 rows instead of
        459,007 - exactly 3x (one real append + 2 duplicate retries).
      - mode="overwrite": raw_iceberg ended up with 918,014 rows instead of
        459,007 - exactly 2x (one real write + 1 duplicate retry). Whatever
        awswrangler does internally for a retried "overwrite" against a
        table that was JUST created by the previous (apparently-failed)
        attempt, it evidently does NOT cleanly replace that data - it adds
        to it. So "overwrite" is not inherently retry-safe either, despite
        being logically idempotent when it runs exactly once.

    Fixed the same way for both: snapshot the target table's row count
    before the attempt, and - only on the propagation-lag error - check
    whether the row count already reflects a successful write of THIS call
    before deciding to retry:
      - append  -> safe if after_count >= before_count + expected_new_rows
                   (the new rows are already there, on top of what existed)
      - overwrite -> safe if after_count == expected_new_rows (the table
                   now holds EXACTLY the new snapshot, matching what a
                   single successful overwrite should produce)
    If the count doesn't match either shape, the write genuinely didn't
    land as expected and it's retried as before.
    """
    logger = get_logger("to_iceberg_retry")
    mode = kwargs.get("mode")
    table = kwargs.get("table")
    df = kwargs.get("df")
    expected_new_rows = len(df) if df is not None else None

    before_count = None
    if mode in ("append", "overwrite") and table and expected_new_rows and _iceberg_table_exists(table):
        before_count = _iceberg_row_count(table)

    for attempt in range(1, ICEBERG_PROPAGATION_RETRIES + 1):
        try:
            wr.athena.to_iceberg(**kwargs)
            return
        except QueryFailed as exc:
            if "cannot find the requested entity" not in str(exc).lower():
                raise  # a different failure - don't mask it, fail immediately

            if expected_new_rows and _iceberg_table_exists(table):
                after_count = _iceberg_row_count(table)
                write_already_landed = (
                    (mode == "append" and before_count is not None and after_count >= before_count + expected_new_rows)
                    or (mode == "overwrite" and after_count == expected_new_rows)
                )
                if write_already_landed:
                    logger.warning(
                        "%s already reflects this write (mode=%s, %s -> %d rows) despite the propagation-lag "
                        "error below - NOT retrying, to avoid duplicating the data: %s",
                        table, mode, before_count, after_count, exc,
                    )
                    return

            if attempt == ICEBERG_PROPAGATION_RETRIES:
                raise
            logger.warning(
                "Iceberg catalog propagation lag (attempt %d/%d) - retrying in %ds: %s",
                attempt, ICEBERG_PROPAGATION_RETRIES, ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS, exc,
            )
            time.sleep(ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS)


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
    called). For raw/cleaned/transformed, use overwrite_iceberg() (or
    merge_iceberg()) instead so reruns don't create duplicate rows.

    schema_evolution=True: UNLIKE overwrite_iceberg() (which drops and
    recreates its target every run, so its schema is always exactly
    whatever today's dataframe has), these are APPEND-ONLY tables that
    accumulate history across runs - failed_iceberg's whole point is to
    keep every past reject on record, so it can't be dropped and recreated
    the way a full-load table can. That means if a job later starts
    producing new columns (e.g. job_3 adding remaining_lease_years/
    remaining_lease_months to the rows it routes to failed_iceberg), a
    plain append against the OLD schema fails with "Schema change
    detected" instead of just picking up the new columns - hit on a real
    run. schema_evolution=True tells to_iceberg() to ADD new columns to the
    table (existing rows get NULL there) rather than reject the write -
    preserves history, unlike overwrite_iceberg()'s drop, while still
    tolerating the table growing new columns over time."""
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    _to_iceberg_with_retry(
        df=df,
        database=GLUE_DATABASE,
        table=table,
        table_location=location,
        keep_files=False,
        mode=mode,
        schema_evolution=True,
        temp_path=ICEBERG_TEMP_LOCATION,
        s3_output=ATHENA_RESULTS_LOCATION,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=_BOTO3_SESSION,
    )


def execute_athena_sql(sql: str, description: str) -> None:
    """Run an arbitrary Athena SQL statement (DDL/DML) and block until done.

    Needed for MERGE/UPDATE against Iceberg tables, which awswrangler's
    to_iceberg() dataframe writer doesn't support directly. Requires an
    Athena workgroup on engine version 3.
    """
    athena = _BOTO3_SESSION.client("athena")
    logger = get_logger("athena_sql")
    logger.info("Athena SQL: %s", description)

    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
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


def _iceberg_row_count(table: str) -> int:
    """Lightweight COUNT(*) via Athena - used only for the before/after
    row-count print in merge_iceberg(). Deliberately not read_iceberg(),
    which pulls the WHOLE table into a dataframe just to call len() on it -
    wasteful for a 400K+-row table when all we want here is a number."""
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=f'SELECT COUNT(*) AS cnt FROM "{GLUE_DATABASE}"."{table}"',
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Row count failed for {table}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return int(rows[1]["Data"][0]["VarCharValue"])  # rows[0] is the header row


def get_table_parameter(table_id: int, parameter_name: str, default: str = None) -> str:
    """Read one parameter_value from table_parameters (seeded by
    01_metadata_setup.py) for a given table_id - e.g.
    get_table_parameter(1, "load_type") -> "FULL" or "MERGE". This is the
    SAME table_parameters row the notebook's context tables already show;
    reading it here lets a job's write strategy be driven by metadata
    instead of hardcoded in the job's own source.

    Returns `default` (without raising) if no matching row exists, so a
    stage still works before 01_metadata_setup.py has been re-run with a
    newer seed - pass default=None (the default) to raise instead."""
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=(
            f'SELECT parameter_value FROM "{GLUE_DATABASE}"."table_parameters" '
            f"WHERE table_id = {int(table_id)} AND parameter_name = '{parameter_name}'"
        ),
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Failed to read table_parameters[{parameter_name}] for table_id={table_id}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    if len(rows) < 2:  # rows[0] is always just the header
        if default is not None:
            return default
        raise KeyError(f"No table_parameters row for table_id={table_id}, parameter_name={parameter_name!r}")
    return rows[1]["Data"][0]["VarCharValue"]


def get_watermark(table_id: int, default: str = None) -> str:
    """Read last_watermark_value from table_watermarks (seeded by
    01_metadata_setup.py, bumped by context_tracking.py/orchestration.py
    after a successful non-FULL run) for a given table_id. Symmetric to
    get_table_parameter() above - same query shape, same
    "return default instead of raising when the row's missing" behaviour -
    just against table_watermarks instead of table_parameters.

    job_1's resolve_effective_date_range() is the first real reader of this:
    for a non-FULL load_type, it narrows the pull window to
    (this watermark - lookback_window days) instead of the full configured
    DATE_RANGE_START..END.

    Returns `default` (without raising) if no matching row exists - pass
    default=None (the default) to raise instead."""
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=(
            f'SELECT last_watermark_value FROM "{GLUE_DATABASE}"."table_watermarks" '
            f"WHERE table_id = {int(table_id)}"
        ),
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Failed to read table_watermarks for table_id={table_id}")
        time.sleep(2)

    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    if len(rows) < 2:  # rows[0] is always just the header
        if default is not None:
            return default
        raise KeyError(f"No table_watermarks row for table_id={table_id}")
    value = rows[1]["Data"][0].get("VarCharValue")
    return value if value is not None else default


def write_by_load_type(df: pd.DataFrame, stage: str, table_id: int = 1) -> None:
    """
    Metadata-driven write: reads load_type from table_parameters (the
    metadata catalog seeded by 01_metadata_setup.py) and dispatches to
    overwrite_iceberg() for load_type='FULL', or merge_iceberg() for
    load_type='MERGE'/'INCREMENTAL'/'UPSERT'. This is what makes "full load
    -> truncate & reload, incremental load -> merge/append" a METADATA
    decision (one row in table_parameters) rather than something baked
    into each job_*.py file - change the row, not the code, if a source's
    load pattern ever changes.

    Defaults to 'FULL' if table_parameters has no row for this table_id
    (matches this pipeline's actual behaviour today - see
    overwrite_iceberg()'s docstring for why FULL is correct here).
    """
    load_type = get_table_parameter(table_id, "load_type", default="FULL").strip().upper()
    logger = get_logger("write_by_load_type")

    if load_type == "FULL":
        logger.info("table_id=%d load_type=FULL -> overwrite_iceberg(%s)", table_id, stage)
        overwrite_iceberg(df, stage)
    elif load_type in ("MERGE", "INCREMENTAL", "UPSERT"):
        logger.info("table_id=%d load_type=%s -> merge_iceberg(%s)", table_id, load_type, stage)
        merge_iceberg(df, stage)
    else:
        raise ValueError(
            f"Unknown load_type {load_type!r} in table_parameters for table_id={table_id} "
            f"(stage={stage!r}) - expected 'FULL' or 'MERGE'/'INCREMENTAL'/'UPSERT'"
        )


def merge_iceberg(df: pd.DataFrame, stage: str, key_column: str = "surrogate_key") -> None:
    """
    Idempotent upsert: write df to a staging Iceberg table, then MERGE INTO
    the target table on key_column. Same input on a rerun -> matched rows
    get UPDATEd in place (no duplication); genuinely new rows get INSERTed.

    NOT currently used by raw/cleaned/transformed - those switched to
    overwrite_iceberg() (see its docstring) once we recognised job_1's
    source pull is always a FULL snapshot, not incremental deltas, which
    makes "replace everything" simpler and safer than an upsert. Left here,
    working and tested, in case a future source becomes genuinely
    incremental (e.g. job_1 starts pulling only rows since the last
    watermark) - that's exactly the case this function is for.
    """
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    staging_table = f"{table}_staging"
    staging_location = location.rstrip("/") + "_staging/"

    # MERGE INTO requires the TARGET table to already exist - unlike a plain
    # to_iceberg() write, Athena/Trino's MERGE won't auto-create it. On the
    # very first run for a stage, the target genuinely doesn't exist yet,
    # which raised: "Table raw_iceberg not found in database ...". Falling
    # back to a plain initial write (which DOES auto-create the table from
    # the dataframe's own schema) sidesteps that - there's nothing to
    # "merge" against yet anyway, so a plain write is equivalent here.
    logger = get_logger("merge_iceberg")

    if not _iceberg_table_exists(table):
        print(f"{table} BEFORE: table does not exist yet (0 rows)")
        logger.info("%s doesn't exist yet - writing initial data directly (no MERGE needed on a first run)", table)
        _to_iceberg_with_retry(
            df=df,
            database=GLUE_DATABASE,
            table=table,
            table_location=location,
            keep_files=False,
            mode="append",
            temp_path=ICEBERG_TEMP_LOCATION,
            s3_output=ATHENA_RESULTS_LOCATION,
            workgroup=ATHENA_WORKGROUP,
            boto3_session=_BOTO3_SESSION,
        )
        print(f"{table} AFTER:  {_iceberg_row_count(table)} row(s) (initial write, no merge)")
        return

    before_count = _iceberg_row_count(table)
    print(f"{table} BEFORE merge: {before_count} row(s)")

    _to_iceberg_with_retry(
        df=df,
        database=GLUE_DATABASE,
        table=staging_table,
        table_location=staging_location,
        keep_files=True,
        mode="overwrite",  # staging table only ever holds this run's batch
        temp_path=ICEBERG_TEMP_LOCATION,
        s3_output=ATHENA_RESULTS_LOCATION,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=_BOTO3_SESSION,
    )

    all_cols = [c for c in df.columns if c != key_column]
    # Trino/Athena's MERGE ... WHEN MATCHED THEN UPDATE SET grammar wants
    # BARE (unqualified) target column names on the left of "=" - only the
    # right-hand side can be alias-qualified ("col = s.col", never
    # "t.col = s.col"). Qualifying the left side raised exactly this:
    # InvalidRequestException: "mismatched input '.'. Expecting: '='"
    # (the parser reads "t", expects "=" right after, chokes on the ".").
    update_set = ", ".join(f"{c} = s.{c}" for c in all_cols)
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

    after_count = _iceberg_row_count(table)
    print(f"{table} AFTER merge:  {after_count} row(s) (was {before_count}, delta {after_count - before_count:+d})")


def _drop_iceberg_table(table: str, location: str) -> None:
    """Fully removes an Iceberg table: deregisters it from the Glue Catalog,
    then deletes every object under its S3 location. Used by
    overwrite_iceberg() to give a full-load stage a GENUINE drop-and-
    recreate every run, not just a row-level replace against whatever
    schema happened to be sitting there before.

    Why this matters: plain to_iceberg(mode="overwrite") refuses to change
    an EXISTING table's schema - it replaces rows, not structure. On a real
    run, cleaned_iceberg already existed in AWS from earlier testing with
    an older schema (missing 2 columns job_3 now computes), and the write
    failed with "Schema change detected" instead of just picking up the new
    columns. Dropping first makes that whole bug class impossible: a
    full-load table's schema always matches EXACTLY what today's job
    produced, because the table is rebuilt from scratch every run."""
    glue = _BOTO3_SESSION.client("glue")
    try:
        glue.delete_table(DatabaseName=GLUE_DATABASE, Name=table)
    except glue.exceptions.EntityNotFoundException:
        pass

    s3 = _BOTO3_SESSION.client("s3")
    bucket, _, prefix = location.replace("s3://", "", 1).partition("/")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def overwrite_iceberg(df: pd.DataFrame, stage: str) -> None:
    """
    Full-load replace: DROPS the target table entirely (Glue Catalog entry
    + underlying S3 data, via _drop_iceberg_table()) if it exists, then
    writes df as a brand-new table via to_iceberg(mode="overwrite"). This is
    a genuine truncate-and-reload, not just a row-level replace - see
    _drop_iceberg_table()'s docstring for why that distinction matters
    (schema drift between runs, discovered via a real "Schema change
    detected" failure on cleaned_iceberg).

    Use this (not merge_iceberg()) for stages that are ALWAYS a complete
    re-load of the same source, never a partial delta. That's every stage in
    this pipeline except hashed_iceberg: job_1 re-downloads the FULL "Resale
    Flat Prices" collection for DATE_RANGE_START..END on every run - it is
    not incremental, there is no "only new rows since last time" - so
    raw_iceberg / cleaned_iceberg / transformed_iceberg are always being
    rebuilt from a complete snapshot. "Delete everything, load the fresh
    snapshot" is both simpler and safer than an upsert here:
      - CORRECTNESS, not just convenience: a MERGE-based upsert only ever
        INSERTs a new key or UPDATEs a matched key - it never removes a row
        whose key disappeared from the new batch. If the source (data.gov.sg)
        ever corrects or retracts a historical record, an upsert would leave
        the old, now-wrong row sitting in the table forever. A full overwrite
        always reflects exactly what the source has right now, because it
        doesn't reconcile row-by-row - it replaces the whole table.
      - No staging table + hand-written MERGE SQL to get wrong (this project
        hit 3 separate MERGE-syntax/semantics bugs this session alone:
        qualified column names, target table not pre-existing, and
        MERGE_TARGET_ROW_MULTIPLE_MATCHES from leftover duplicate keys).
      - No "does the target exist yet" special case - mode="overwrite"
        creates the table on a first run exactly like mode="append" did.
      - Retry-safe BY CONSTRUCTION: unlike append, replaying an overwrite
        with the SAME dataframe produces the SAME end state, not duplicate
        rows. _to_iceberg_with_retry()'s append-only duplication guard
        (added after the 3x row-count bug) simply doesn't apply to this
        mode - there's nothing to duplicate.

    hashed_iceberg (job_5) does NOT use this - it's genuine SCD2 versioning
    that compares this run's data against the PRIOR run's is_current rows to
    detect real-world changes; overwriting it would destroy the version
    history it exists to keep. It keeps its own incremental write/merge
    logic untouched.
    """
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    logger = get_logger("overwrite_iceberg")

    before_exists = _iceberg_table_exists(table)
    before_count = _iceberg_row_count(table) if before_exists else 0
    print(f"{table} BEFORE: {before_count} row(s)" if before_exists else f"{table} BEFORE: table does not exist yet (0 rows)")

    if before_exists:
        logger.info("Dropping %s (Glue table + S3 data) before full reload, so its schema always matches this run's dataframe exactly", table)
        _drop_iceberg_table(table, location)

    _to_iceberg_with_retry(
        df=df,
        database=GLUE_DATABASE,
        table=table,
        table_location=location,
        keep_files=False,
        mode="overwrite",
        temp_path=ICEBERG_TEMP_LOCATION,
        s3_output=ATHENA_RESULTS_LOCATION,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=_BOTO3_SESSION,
    )

    after_count = _iceberg_row_count(table)
    print(f"{table} AFTER:  {after_count} row(s) (full reload, was {before_count})")


def read_iceberg(stage: str) -> pd.DataFrame:
    """Read the full contents of a stage's Iceberg table via Athena."""
    table, _ = TABLES[stage]
    return wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{table}"',
        database=GLUE_DATABASE,
        ctas_approach=False,
        s3_output=ATHENA_RESULTS_LOCATION,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=_BOTO3_SESSION,
    )


def athena_read_sql(sql: str) -> pd.DataFrame:
    """Run an arbitrary SELECT via Athena and return it as a dataframe - the
    same s3_output/workgroup/boto3_session wiring read_iceberg() uses, for
    callers that need a query read_iceberg()'s "SELECT * FROM one whole
    stage table" shape doesn't cover (e.g. job_5's read_current_versions(),
    which filters to WHERE is_current = true instead of reading everything).

    Added after a real run showed awswrangler's own warning - "No s3_output
    was provided... falling back to the default bucket" - from a query that
    had been calling wr.athena.read_sql_query() directly without this
    wiring. Functionally harmless (falls back to a usable default bucket),
    but inconsistent with every other Athena call in this file, which all
    point at ATHENA_RESULTS_LOCATION - hence this shared helper instead of
    every caller repeating (or forgetting) the same 3 keyword arguments."""
    return wr.athena.read_sql_query(
        sql=sql,
        database=GLUE_DATABASE,
        ctas_approach=False,
        s3_output=ATHENA_RESULTS_LOCATION,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=_BOTO3_SESSION,
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
# Audit logging (AUDIT_S3_BUCKET, from config.py)
# --------------------------------------------------------------------------- #

def record_audit(job_name: str, stage: str, rows_in: int, rows_out: int, rows_rejected: int) -> None:
    """
    Append one row per job run to the audit_iceberg table: what ran, how many
    rows came in, how many passed, how many were rejected. This is the
    traceability trail referenced by AUDIT_S3_BUCKET - separate
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
# Secrets Manager - for actual credentials, if/when this pipeline ever has any
# --------------------------------------------------------------------------- #

def get_secret(secret_id: str, default: str = None) -> str:
    """Read a secret value from AWS Secrets Manager. NOT currently called by
    any job - data.gov.sg's collection/dataset APIs are public and need no
    API key or token today (job_1_ingestion_to_source.py never sends an
    Authorization header). Kept here, ready to use, so that IF data.gov.sg
    (or any future source) ever requires a real credential, storing and
    reading it is a one-line change - get_secret("hdb/some-api-key") -
    instead of new plumbing. This is the actual-secrets counterpart to
    config.py's Parameter Store config (plain, non-secret runtime values
    like API base URLs) and get_account_id() (identity, resolved live, never
    stored anywhere) - three different tools for three different kinds of
    "don't hardcode this" value.

    Returns `default` (without raising) if the secret doesn't exist / no
    permission - pass default=None (the default) to raise instead."""
    secrets = _BOTO3_SESSION.client("secretsmanager")
    try:
        response = secrets.get_secret_value(SecretId=secret_id)
        return response.get("SecretString", default)
    except Exception:
        if default is not None:
            return default
        raise


# --------------------------------------------------------------------------- #
# Alerting
# --------------------------------------------------------------------------- #

def send_alert(subject: str, message: str) -> None:
    """Publish a run summary / failure alert to SNS.

    The topic ARN is assembled HERE, at call time, from the live account id
    (get_account_id(), resolved via STS - never hardcoded or stored) plus
    AWS_REGION and SNS_TOPIC_NAME (both plain, non-sensitive config from
    config.py, safe to default in source). Nothing account-specific is ever
    written to source code or to Parameter Store - see get_account_id()'s
    docstring for why. SNS_TOPIC_ARN_OVERRIDE (from HDB_SNS_TOPIC_ARN) is an
    escape hatch for the rare case of publishing to a topic in a DIFFERENT
    account/region than this job runs in - unset (the normal case), the ARN
    is always built fresh from this run's own identity."""
    sns = _BOTO3_SESSION.client("sns")
    topic_arn = SNS_TOPIC_ARN_OVERRIDE or f"arn:aws:sns:{AWS_REGION}:{get_account_id()}:{SNS_TOPIC_NAME}"
    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    print(f"Alert sent: {subject}")
