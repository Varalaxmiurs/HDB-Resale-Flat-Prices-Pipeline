"""
common.py
=========
Shared helper functions for the HDB Resale Flat Prices pipeline. Imported
by every Glue job script (job_1 .. job_5) so that Iceberg read/write,
surrogate-key, audit-logging, and alerting logic is defined once.

All configurable values (Glue database, Athena workgroup, S3 buckets, the
SNS topic ARN, the natural key columns, etc.) live in config.py - the
pipeline's single source of truth - and are imported here rather than
duplicated.

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
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import pandas as pd

from config import (
    ATHENA_WORKGROUP,
    AUDIT_S3_BUCKET,
    AWS_REGION,
    COLUMN_TYPES,
    GLUE_DATABASE,
    MAX_CONCURRENT_ATHENA_INSERTS,
    MAX_CONCURRENT_S3_READS,
    NATURAL_KEY_COLUMNS,
    SNS_TOPIC_ARN_OVERRIDE,
    SNS_TOPIC_NAME,
    TABLES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

for _noisy_logger in ("boto3", "botocore", "urllib3", "s3transfer", "awswrangler"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

_BOTO3_SESSION = boto3.Session(region_name=AWS_REGION)

ATHENA_RESULTS_LOCATION = f"s3://{AUDIT_S3_BUCKET}/athena-results/"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


_account_id_cache = None


def get_account_id() -> str:
    """Live account id via STS GetCallerIdentity - never hardcoded, never
    written to a file. Cached per-process."""
    global _account_id_cache
    if _account_id_cache is None:
        _account_id_cache = _BOTO3_SESSION.client("sts").get_caller_identity()["Account"]
    return _account_id_cache


def _iceberg_table_exists(table: str) -> bool:
    """Plain Glue Catalog lookup (get_table) rather than asking Athena -
    Athena's own CREATE TABLE IF NOT EXISTS re-validates an existing
    table's Iceberg metadata even as a no-op, which has intermittently
    raised spurious "cannot find the requested entity" errors."""
    glue = _BOTO3_SESSION.client("glue")
    try:
        glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


ICEBERG_PROPAGATION_RETRIES = 3
ICEBERG_PROPAGATION_RETRY_DELAY_SECONDS = 8


def _athena_type_for_series(series: pd.Series) -> str:
    """pandas dtype -> Athena/Iceberg column type (Iceberg-specific types
    like STRING/BIGINT, not generic Trino VARCHAR/LONG - see AWS's
    Iceberg docs). Falls back to STRING for anything unmatched.

    This is a LAST-RESORT fallback for a column nobody has named yet -
    see _canonical_athena_type() below for the layered lookup every real
    write path (_create_iceberg_table_sql, _ensure_iceberg_columns,
    _insert_iceberg_rows) actually uses; none of them should call this
    directly for a column that config.COLUMN_TYPES or the target table's
    existing schema already has an answer for."""
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "STRING"


def _canonical_athena_type(col: str, series: pd.Series, existing_types: dict = None) -> str:
    """The single place every write path asks "what Athena type is this
    column?" - checked in this order, each one only consulted if the
    previous had no answer:

      1. config.COLUMN_TYPES - the hand-maintained, permanent registry of
         what a column NAMED this always means. Immune to what any one
         batch of data happens to look like - this is what actually fixes
         the recurring TYPE_MISMATCH bug (see COLUMN_TYPES' own BUGFIX
         comment in config.py for the full story: a free-text column like
         `block` can look like an integer in a batch where every value
         happens to be all-digits, and a per-batch guess would then
         disagree with what the column has always been elsewhere).
      2. `existing_types` - the target Iceberg table's OWN already-
         established Glue Catalog schema, when the caller has it (pass
         _existing_iceberg_column_types(table) here). Covers a column
         that predates COLUMN_TYPES being filled in, or one deliberately
         left out of it.
      3. _athena_type_for_series(series) - infer fresh from this batch's
         pandas dtype. Only reached for a column that is genuinely new to
         both the registry and the table - i.e. there is no established
         answer anywhere yet, so this batch's own data is the only
         information available."""
    canonical = COLUMN_TYPES.get(col.lower())
    if canonical:
        return canonical
    if existing_types:
        existing = existing_types.get(col.lower())
        if existing:
            return existing
    return _athena_type_for_series(series)


def _sql_literal(value, athena_type: str) -> str:
    """One Python value -> its Athena SQL literal, typed per athena_type.
    CAST(NULL AS <type>) for anything pandas considers missing
    (None/NaN/NaT/pd.NA) - see the BUGFIX comment inline below for why a
    bare NULL/numeric literal isn't safe against an already-existing
    target column.

    pd.api.types.is_scalar() guards the pd.isna() call below: on a list/
    array value, pd.isna() returns an element-wise array rather than
    raising, and bool()'ing that array in a plain `if` is what actually
    raises "truth value of an array is ambiguous" - guarding with
    is_scalar() first means a list/array value is simply treated as
    present instead."""
    if not pd.api.types.is_scalar(value):
        is_missing = False
    else:
        try:
            is_missing = value is None or pd.isna(value)
        except (TypeError, ValueError):
            is_missing = False
    if is_missing:
        # CAST(NULL AS <type>), not bare NULL - see BUGFIX comment above
        # _sql_literal's docstring: a bare NULL literal has no type of its
        # own (Athena/Trino reports it as "unknown" when checking the
        # VALUES clause against the target table), which fails
        # TYPE_MISMATCH against a concrete-typed column even though NULL
        # is legitimately assignable to it. Casting pins the type so the
        # check passes.
        return f"CAST(NULL AS {athena_type})"

    if athena_type == "BOOLEAN":
        return "true" if value else "false"
    if athena_type == "TIMESTAMP":
        ts = pd.Timestamp(value)
        return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
    if athena_type in ("BIGINT", "DOUBLE"):
        # BUGFIX: a bare numeric literal like "1980" is parsed by
        # Athena/Trino as INTEGER (or DECIMAL for a plain "41.0"), never
        # as BIGINT/DOUBLE, regardless of what type this column is
        # *meant* to be - so an un-cast literal can TYPE_MISMATCH against
        # an existing BIGINT/DOUBLE column even when the Python value and
        # the target type actually agree. Explicit CAST forces the
        # literal's SQL type to match.
        return f"CAST({value} AS {athena_type})"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _create_iceberg_table_sql(table: str, location: str, df: pd.DataFrame) -> str:
    """Athena's Iceberg CREATE TABLE syntax: unquoted (bare) column names,
    LOCATION/TBLPROPERTIES (not WITH (...), which is CTAS-only), matching
    both a working reference pipeline and this account's own
    Athena-reconstructed DDL for an existing table. None of this
    pipeline's column names are reserved words, so leaving them unquoted
    is safe here."""
    col_types = {c: _canonical_athena_type(c, df[c]) for c in df.columns}
    cols_sql = ",\n    ".join(f'{c} {t}' for c, t in col_types.items())
    return f"""
    CREATE TABLE IF NOT EXISTS {GLUE_DATABASE}.{table} (
        {cols_sql}
    )
    LOCATION '{location}'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet',
        'write_compression' = 'snappy'
    )
    """


ICEBERG_INSERT_MAX_PAYLOAD_BYTES = 230_000


_ICEBERG_CONFLICT_PATTERNS = ("conflict", "concurrent", "commit")


def _execute_athena_insert_batch(sql: str, description: str, max_retries: int = 4) -> None:
    """execute_athena_sql(), with retry-on-conflict for the concurrent-
    commit case parallel INSERT batches make possible (see
    MAX_CONCURRENT_ATHENA_INSERTS in config.py). A non-conflict failure
    still raises immediately on the first attempt."""
    for attempt in range(1, max_retries + 1):
        try:
            execute_athena_sql(sql, description)
            return
        except RuntimeError as exc:
            is_conflict = any(p in str(exc).lower() for p in _ICEBERG_CONFLICT_PATTERNS)
            if not is_conflict or attempt == max_retries:
                raise
            wait_seconds = 1.5 * (2 ** (attempt - 1)) + random.uniform(0, 1)
            get_logger("athena_sql").warning(
                "Possible concurrent-commit conflict on %s (attempt %d/%d) - retrying in %.1fs: %s",
                description, attempt, max_retries, wait_seconds, exc,
            )
            time.sleep(wait_seconds)


def _insert_iceberg_rows(table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    # Prefer the target table's OWN already-established column types
    # (see _existing_iceberg_column_types' BUGFIX docstring) - only a
    # column the table has never seen falls back to this batch's own
    # dtype, since _ensure_iceberg_columns() (called before this, in
    # _write_df_iceberg_raw) will just have ALTER TABLE'd it into
    # existence using that same fallback type.
    existing_types = _existing_iceberg_column_types(table) or {}
    col_types = {
        c: _canonical_athena_type(c, df[c], existing_types)
        for c in df.columns
    }
    columns = list(df.columns)
    cols_sql = ", ".join(columns)
    prefix = f"INSERT INTO {GLUE_DATABASE}.{table} ({cols_sql}) VALUES\n"
    max_values_bytes = ICEBERG_INSERT_MAX_PAYLOAD_BYTES - len(prefix.encode("utf-8"))

    row_strings = [
        "(" + ", ".join(_sql_literal(val, col_types[col]) for col, val in zip(columns, row)) + ")"
        for row in df.itertuples(index=False, name=None)
    ]

    batches = []
    current, current_bytes = [], 0
    for row_str in row_strings:
        row_bytes = len(row_str.encode("utf-8")) + 2
        if current and current_bytes + row_bytes > max_values_bytes:
            batches.append(current)
            current, current_bytes = [], 0
        current.append(row_str)
        current_bytes += row_bytes
    if current:
        batches.append(current)

    n_batches = len(batches)

    def _run_batch(i: int, batch_rows: list) -> None:
        values_sql = ",\n".join(batch_rows)
        sql = f"{prefix}{values_sql}"
        _execute_athena_insert_batch(
            sql, f"INSERT batch {i + 1}/{n_batches} into {table} ({len(batch_rows)} row(s))"
        )

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ATHENA_INSERTS) as pool:
        futures = [pool.submit(_run_batch, i, batch) for i, batch in enumerate(batches)]
        for future in futures:
            future.result()


def _existing_iceberg_columns(table: str):
    """Lowercase column names Glue's catalog currently has for `table`, or
    None if the table doesn't exist yet."""
    glue = _BOTO3_SESSION.client("glue")
    try:
        tbl = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    return {c["Name"].lower() for c in tbl["StorageDescriptor"]["Columns"]}


_HIVE_TYPE_TO_ATHENA_TYPE = {
    "string": "STRING",
    "varchar": "STRING",
    "bigint": "BIGINT",
    "int": "BIGINT",
    "integer": "BIGINT",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}


def _existing_iceberg_column_types(table: str):
    """Lowercase column name -> our internal Athena type name (STRING/
    BIGINT/DOUBLE/BOOLEAN/TIMESTAMP), read from the table's ALREADY
    ESTABLISHED Glue Catalog schema - or None if the table doesn't exist
    yet.

    BUGFIX: _insert_iceberg_rows() used to derive every column's literal
    type fresh from the CURRENT batch's pandas dtype
    (_athena_type_for_series), on every single write. That's fine the
    first time a table is created, but an append-only history table
    (failed_iceberg, audit_iceberg, ...) has a schema fixed at creation -
    and a later batch's dtype for the same column can legitimately differ
    from what was first inferred. Concretely: 'block' is a free-text HDB
    field (values like '406' or '406A'); a batch where every block value
    happens to be all-digits gets read back from Iceberg/pandas as an
    integer dtype, so a naive per-batch type lookup emits a bare integer
    literal - even though the table's real column type, fixed back when
    it was first created against a batch that HAD a letter suffix, is
    STRING. Athena then rejects the whole INSERT with TYPE_MISMATCH,
    which took down job_3 entirely (including its otherwise-valid rows)
    over a single mistyped reject row.

    The fix: for any column that already exists on the target table, use
    THIS (the table's real, fixed) type as the source of truth for its
    SQL literal - not a fresh per-batch guess. Only a column the target
    table has never seen before falls back to _athena_type_for_series()
    (see _insert_iceberg_rows / _ensure_iceberg_columns), since there's no
    established type to defer to yet."""
    glue = _BOTO3_SESSION.client("glue")
    try:
        tbl = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    types = {}
    for c in tbl["StorageDescriptor"]["Columns"]:
        raw_type = c["Type"].lower().split("(")[0].strip()  # e.g. "varchar(64)" -> "varchar"
        types[c["Name"].lower()] = _HIVE_TYPE_TO_ATHENA_TYPE.get(raw_type, "STRING")
    return types


def _ensure_iceberg_columns(table: str, df: pd.DataFrame) -> None:
    """Lightweight schema evolution: ALTER TABLE ADD COLUMNS for any df
    column not already present on the target table. Needed for append-only
    history tables (failed_iceberg, audit_iceberg, hashed_iceberg) that can
    receive a wider column set over time than whatever schema first
    created them - CREATE TABLE IF NOT EXISTS is a no-op against an
    already-existing table, so it can never add columns on its own.

    Uses Athena's Iceberg-specific "ADD COLUMNS (col type, ...)" (plural,
    with parens) - generic Trino singular "ADD COLUMN" is rejected here."""
    existing = _existing_iceberg_columns(table)
    if existing is None:
        return
    col_types = {c: _canonical_athena_type(c, df[c]) for c in df.columns}
    missing = [(c, t) for c, t in col_types.items() if c.lower() not in existing]
    if not missing:
        return
    cols_sql = ", ".join(f"{c} {t}" for c, t in missing)
    execute_athena_sql(
        f"ALTER TABLE {GLUE_DATABASE}.{table} ADD COLUMNS ({cols_sql})",
        f"ALTER TABLE {table} ADD COLUMNS ({cols_sql})",
    )


def _write_df_iceberg_raw(df: pd.DataFrame, table: str, location: str, mode: str) -> None:
    """CREATE (if needed) + evolve schema (if needed) + INSERT.
    mode="overwrite" assumes the caller already dropped any existing
    table (see overwrite_iceberg()), so this only ever CREATEs fresh +
    INSERTs; mode="append" CREATEs on a first run or evolves + INSERTs
    into an existing table."""
    execute_athena_sql(_create_iceberg_table_sql(table, location, df), f"CREATE TABLE IF NOT EXISTS {table}")
    _ensure_iceberg_columns(table, df)
    _insert_iceberg_rows(table, df)


def _to_iceberg_with_retry(df: pd.DataFrame, table: str, location: str, mode: str) -> None:
    """Retrying a write is not automatically safe: the propagation-lag
    "cannot find the requested entity" error can surface AFTER the actual
    data write already committed, so a naive blind retry can duplicate it.
    Confirmed for real with the old writer: mode="append" once produced
    1,377,021 rows instead of 459,007 (exactly 3x - one real append plus 2
    duplicate retries), and mode="overwrite" once produced 918,014 instead
    of 459,007 (exactly 2x).

    Fixed by snapshotting the target table's row count before the
    attempt, and - only on the propagation-lag error - checking whether
    the count already reflects a successful write of THIS call:
      - append    -> safe if after_count >= before_count + expected_new_rows
      - overwrite -> safe if after_count == expected_new_rows
    Otherwise the write genuinely didn't land and is retried as before."""
    logger = get_logger("to_iceberg_retry")
    expected_new_rows = len(df) if df is not None else None

    before_count = None
    if mode in ("append", "overwrite") and table and expected_new_rows and _iceberg_table_exists(table):
        before_count = _iceberg_row_count(table)

    for attempt in range(1, ICEBERG_PROPAGATION_RETRIES + 1):
        try:
            _write_df_iceberg_raw(df, table, location, mode)
            return
        except RuntimeError as exc:
            if "cannot find the requested entity" not in str(exc).lower():
                raise

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


def compute_surrogate_key(df: pd.DataFrame, key_columns: list = None) -> pd.Series:
    """Deterministic SHA-256 surrogate key from the natural/composite key
    columns - the same input row always yields the same key, which is what
    makes MERGE-based upserts idempotent across reruns.

    A list comprehension over raw Python strings, not
    Series.apply(hashlib.sha256...) - apply() carries real per-row
    overhead that adds up fast at 100Ks-1M+ rows; this is a speed fix
    only, output is unchanged."""
    key_columns = key_columns or [c for c in NATURAL_KEY_COLUMNS if c in df.columns]
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise KeyError(f"Cannot compute surrogate key - missing columns: {missing}")
    concat = df[key_columns].astype(str).agg("|".join, axis=1)
    hashed = [hashlib.sha256(s.encode("utf-8")).hexdigest() for s in concat]
    return pd.Series(hashed, index=df.index)


def write_iceberg(df: pd.DataFrame, stage: str, mode: str = "append") -> None:
    """Write a dataframe to the named Iceberg stage table. No-op on empty df.

    Plain append - only safe for tables where every write is genuinely new
    data (failed_iceberg, audit_iceberg, or hashed_iceberg's already-
    deduplicated SCD2 versions). For raw/cleaned/transformed, use
    overwrite_iceberg() instead so reruns don't create duplicate rows.

    schema_evolution=True: unlike overwrite_iceberg() (which drops and
    recreates its target every run), these are append-only tables that
    accumulate history and can't be dropped and recreated - if a job later
    adds new columns, a plain append against the old schema fails with
    "Schema change detected" instead of picking up the new columns."""
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    _to_iceberg_with_retry(df=df, table=table, location=location, mode=mode)


def execute_athena_sql(sql: str, description: str) -> None:
    """Run an arbitrary Athena SQL statement (DDL/DML) and block until
    done. Needed for MERGE/UPDATE against Iceberg tables. Requires an
    Athena workgroup on engine version 3."""
    athena = _BOTO3_SESSION.client("athena")
    logger = get_logger("athena_sql")
    print(f"Athena SQL: {description}")

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


def _read_one_csv_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Download + parse a single CSV object. Split out of
    read_csv_files_from_s3() so it can run inside a thread-pool worker."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    return pd.read_csv(body)


def read_csv_files_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    """Read and concatenate every .csv object under an S3 prefix into one
    dataframe.

    Files are downloaded+parsed concurrently (up to
    config.MAX_CONCURRENT_S3_READS at once) - these are our own S3
    objects with no shared rate limit to worry about, unlike job_1's calls
    to the external data.gov.sg API, so a higher concurrency is safe.

    Source files are left in place after being read (no archiving) -
    rerunning against the same files simply reprocesses them, which is
    safe since raw_iceberg is a full overwrite every run."""
    s3 = _BOTO3_SESSION.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    if not keys:
        return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_S3_READS) as pool:
        frames = list(pool.map(lambda key: _read_one_csv_from_s3(s3, bucket, key), keys))
    return pd.concat(frames, ignore_index=True)


def _iceberg_row_count(table: str) -> int:
    """Lightweight COUNT(*) via Athena, for the before/after row-count
    print in merge_iceberg()/overwrite_iceberg() - not read_iceberg(),
    which would pull the whole table into a dataframe just to call len()."""
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
    return int(rows[1]["Data"][0]["VarCharValue"])


def get_table_parameter(table_id: int, parameter_name: str, default: str = None) -> str:
    """Read one parameter_value from table_parameters (seeded by
    01_metadata_setup.py) for a given table_id, e.g.
    get_table_parameter(1, "load_type") -> "FULL" or "MERGE" - lets a
    job's write strategy be driven by metadata instead of hardcoded.

    Returns `default` (without raising) if no matching row exists - pass
    default=None (the default) to raise instead."""
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
    if len(rows) < 2:
        if default is not None:
            return default
        raise KeyError(f"No table_parameters row for table_id={table_id}, parameter_name={parameter_name!r}")
    return rows[1]["Data"][0]["VarCharValue"]


def get_watermark(table_id: int, default: str = None) -> str:
    """Read last_watermark_value from table_watermarks for a given
    table_id - symmetric to get_table_parameter() above. job_1's
    resolve_effective_date_range() is the first real reader: for a
    non-FULL load_type, it narrows the pull window to
    (this watermark - lookback_window days).

    Returns `default` (without raising) if no matching row exists."""
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
    if len(rows) < 2:
        if default is not None:
            return default
        raise KeyError(f"No table_watermarks row for table_id={table_id}")
    value = rows[1]["Data"][0].get("VarCharValue")
    return value if value is not None else default


def write_by_load_type(df: pd.DataFrame, stage: str, table_id: int = 1) -> None:
    """Metadata-driven write: reads load_type from table_parameters and
    dispatches to overwrite_iceberg() for 'FULL', or merge_iceberg() for
    'MERGE'/'INCREMENTAL'/'UPSERT' - a load pattern change is then one row
    in table_parameters, not a code change. Defaults to 'FULL' if no row
    exists for this table_id."""
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
    """Idempotent upsert: write df to a staging Iceberg table, then MERGE
    INTO the target table on key_column - a rerun of the same input
    UPDATEs matched rows in place instead of duplicating them.

    Not currently used by raw/cleaned/transformed (they use
    overwrite_iceberg() instead, since job_1's source pull is always a
    full snapshot, not incremental deltas). Left here for a future source
    that becomes genuinely incremental."""
    if df is None or df.empty:
        return
    table, location = TABLES[stage]
    staging_table = f"{table}_merge_scratch"
    staging_location = location.rstrip("/") + "_merge_scratch/"

    logger = get_logger("merge_iceberg")

    if not _iceberg_table_exists(table):
        print(f"{table} BEFORE: table does not exist yet (0 rows)")
        logger.info("%s doesn't exist yet - writing initial data directly (no MERGE needed on a first run)", table)
        _to_iceberg_with_retry(df=df, table=table, location=location, mode="append")
        print(f"{table} AFTER:  {_iceberg_row_count(table)} row(s) (initial write, no merge)")
        return

    before_count = _iceberg_row_count(table)
    print(f"{table} BEFORE merge: {before_count} row(s)")

    _ensure_iceberg_columns(table, df)

    if _iceberg_table_exists(staging_table):
        _drop_iceberg_table(staging_table, staging_location)

    _to_iceberg_with_retry(
        df=df, table=staging_table, location=staging_location, mode="overwrite"
    )

    all_cols = [c for c in df.columns if c != key_column]
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
    """Fully removes an Iceberg table: deregisters it from the Glue
    Catalog, then deletes every object under its S3 location. Used by
    overwrite_iceberg() for a genuine drop-and-recreate every run, since
    plain to_iceberg(mode="overwrite") replaces rows but refuses to change
    an existing table's schema."""
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
    """Full-load replace: drops the target table entirely (Glue Catalog
    entry + S3 data) if it exists, then writes df as a brand-new table.
    A genuine truncate-and-reload, not a row-level replace.

    Use this (not merge_iceberg()) for a stage that is always a complete
    re-load of the same source - every stage here except hashed_iceberg,
    since job_1 re-downloads the full collection every run rather than an
    incremental delta. Preferred over an upsert for this pipeline because:
      - correctness: an upsert never removes a row whose key disappeared
        from the new batch, so a corrected/retracted source record would
        leave the old row in place forever; a full overwrite always
        reflects exactly what the source has right now.
      - no staging table + hand-written MERGE SQL to get wrong.
      - retry-safe by construction: replaying the same dataframe produces
        the same end state, not duplicate rows.

    hashed_iceberg (job_5) does not use this - it's genuine SCD2
    versioning that compares against the prior run's is_current rows, and
    overwriting it would destroy that version history."""
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

    _to_iceberg_with_retry(df=df, table=table, location=location, mode="overwrite")

    after_count = _iceberg_row_count(table)
    print(f"{table} AFTER:  {after_count} row(s) (full reload, was {before_count})")


def read_iceberg(stage: str) -> pd.DataFrame:
    """Read the full contents of a stage's Iceberg table via Athena."""
    table, _ = TABLES[stage]
    return athena_read_sql(f'SELECT * FROM "{table}"')


_ATHENA_TYPE_TO_PANDAS = {
    "integer": "int", "bigint": "int", "smallint": "int", "tinyint": "int",
    "double": "float", "float": "float", "real": "float", "decimal": "float",
    "boolean": "bool",
    "timestamp": "datetime", "date": "datetime",
}


def athena_read_sql(sql: str) -> pd.DataFrame:
    """Run an arbitrary SELECT via Athena and return it as a dataframe.
    get_query_results() returns every cell as plain text - this re-applies
    real types per-column afterward using Athena's own reported column
    types, so callers get workable int/float/bool/datetime dtypes instead
    of an all-string dataframe."""
    athena = _BOTO3_SESSION.client("athena")
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena SELECT failed: {reason}")
        time.sleep(2)

    columns = None
    col_types = None
    data_rows = []
    paginator = athena.get_paginator("get_query_results")
    for page_num, page in enumerate(paginator.paginate(QueryExecutionId=query_id)):
        result_set = page["ResultSet"]
        if columns is None:
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]
            columns = [c["Name"] for c in column_info]
            col_types = [c["Type"] for c in column_info]
        rows = result_set["Rows"]
        start = 1 if page_num == 0 else 0
        for row in rows[start:]:
            data_rows.append([cell.get("VarCharValue") for cell in row["Data"]])

    df = pd.DataFrame(data_rows, columns=columns or [])
    for col, athena_type in zip(columns or [], col_types or []):
        kind = _ATHENA_TYPE_TO_PANDAS.get(athena_type)
        if kind == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif kind == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif kind == "bool":
            df[col] = df[col].map({"true": True, "false": False})
        elif kind == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def route_to_failed(df: pd.DataFrame, reason: str, stage: str) -> None:
    """Tag rejected records with reason + originating stage and append to failed_iceberg."""
    if df is None or df.empty:
        return
    tagged = df.copy()
    tagged["_failure_reason"] = reason
    tagged["_failed_stage"] = stage
    tagged["_failed_at"] = datetime.utcnow().isoformat()
    write_iceberg(tagged, "failed")


def record_audit(job_name: str, stage: str, rows_in: int, rows_out: int, rows_rejected: int,
                  start_time: datetime = None) -> None:
    """Append one row per job run to audit_iceberg: what ran, how many
    rows came in, how many passed, how many were rejected - the
    traceability trail, separate from failed_iceberg (which holds the
    actual rejected records).

    start_time (optional): the same datetime.utcnow() every job_*.py
    already captures at the top of main() and threads into
    record_stage_result() - passed here too so audit_iceberg carries a
    real duration_seconds for this job's run, instead of only a single
    completion timestamp. None (the default) writes duration_seconds=None
    for any caller that has no start_time to hand.

    Fail-soft: this is a logging nicety, not the job's actual output - the
    real deliverable (raw/cleaned/transformed/hashed_iceberg, via
    write_iceberg()) still raises on failure. Kept as a safety net so an
    audit-table hiccup alone can't take down an otherwise-successful run."""
    now = datetime.utcnow()
    duration_seconds = round((now - start_time).total_seconds(), 3) if start_time is not None else None
    audit_row = pd.DataFrame([{
        "run_id": str(uuid.uuid4()),
        "job_name": job_name,
        "stage": stage,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "run_timestamp": now.isoformat(),
        "duration_seconds": duration_seconds,
    }])
    try:
        write_iceberg(audit_row, stage="audit", mode="append")
    except Exception as exc:
        get_logger("record_audit").warning(
            "record_audit() failed to write audit_iceberg (non-fatal, job continues): %s", exc
        )


def get_secret(secret_id: str, default: str = None) -> str:
    """Read a secret value from AWS Secrets Manager. Not currently called
    by any job - data.gov.sg's APIs are public and need no API key today.
    Kept ready so a future credential requirement is a one-line change
    (get_secret("hdb/some-api-key")) instead of new plumbing.

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


def send_alert(subject: str, message: str) -> None:
    """Publish a run summary / failure alert to SNS. The topic ARN is
    assembled here at call time from the live account id
    (get_account_id(), via STS - never hardcoded) plus AWS_REGION and
    SNS_TOPIC_NAME. SNS_TOPIC_ARN_OVERRIDE is an escape hatch for
    publishing to a topic in a different account/region than this job
    runs in - unset in the normal case."""
    sns = _BOTO3_SESSION.client("sns")
    topic_arn = SNS_TOPIC_ARN_OVERRIDE or f"arn:aws:sns:{AWS_REGION}:{get_account_id()}:{SNS_TOPIC_NAME}"
    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    print(f"Alert sent: {subject}")
