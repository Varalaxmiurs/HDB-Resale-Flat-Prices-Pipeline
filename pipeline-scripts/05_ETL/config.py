"""
config.py
==========
Single source of truth for every configurable value used across the HDB
pipeline: common.py, context_tracking.py, and job_1 .. job_5. Nothing
below should be hardcoded anywhere else.

Every setting has a sensible default and can be overridden via an
environment variable, so dev/staging/prod can use the same code with
different values.

How to override in AWS Glue:
    Any Job Parameter added as `--HDB_SOME_SETTING value` in the Glue
    Job's "Job parameters" is exposed to the script as an environment
    variable named `HDB_SOME_SETTING` automatically.

How to override locally (Git Bash):
    export HDB_GLUE_DATABASE=my_dev_database
    python job_1_ingestion_to_source.py
"""

import os
import sys

import boto3


def _bridge_glue_job_arguments() -> None:
    """Glue Job Arguments arrive as '--KEY VALUE' pairs in sys.argv, not
    as OS environment variables. Bridges any '--HDB_...' argument into
    the equivalent os.environ entry so every _env()/_env_int() call below
    sees it the same way it would from a local `export`. No-op for local
    runs; setdefault() means a real env var already set wins."""
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("--HDB_") and i + 1 < len(args):
            os.environ.setdefault(arg[2:], args[i + 1])
            i += 2
        else:
            i += 1


_bridge_glue_job_arguments()


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


def _env_list(key: str, default: list) -> list:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return [v.strip() for v in raw.split(",") if v.strip()]


SSM_PARAMETER_PREFIX = _env("HDB_SSM_PREFIX", "/hdb-pipeline")

_ssm_client = None


def _get_ssm_client():
    """Lazy, region left to boto3's own default resolution (this file is
    also imported in contexts, e.g. offline unit tests, where forcing a
    client to exist eagerly would be unwanted)."""
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def _env_or_ssm(env_key: str, ssm_name: str, default: str) -> str:
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val
    try:
        response = _get_ssm_client().get_parameter(Name=ssm_name)
        return response["Parameter"]["Value"]
    except Exception:
        return default


PROJECT_NAME = _env("PROJECT_NAME", _env("HDB_PROJECT_NAME", "hdb-eventdriven"))
_account_id_cache = None


def _get_account_id() -> str:
    """Live account id via STS (same as `aws sts get-caller-identity`),
    duplicated from common.py rather than imported to avoid a circular
    import. Cached per-process. Falls back to a fake placeholder with no
    AWS credentials (offline tests, local dev) rather than crashing every
    job at import time - only a real run needs the real id, used to keep
    the bucket names below globally-unique (S3 bucket names are a global
    namespace)."""
    global _account_id_cache
    if _account_id_cache is None:
        try:
            _account_id_cache = boto3.client("sts").get_caller_identity()["Account"]
        except Exception:
            _account_id_cache = "000000000000"
    return _account_id_cache


_BUCKET_PREFIX = f"mission-{PROJECT_NAME}-{_get_account_id()}"
_DEFAULT_GLUE_DATABASE = f"{PROJECT_NAME.replace('-', '_')}_database"
_DEFAULT_SNS_TOPIC_NAME = f"{PROJECT_NAME}-notifications"


AWS_REGION = "us-east-1"

GLUE_DATABASE = _env("HDB_GLUE_DATABASE", _DEFAULT_GLUE_DATABASE)
ATHENA_WORKGROUP = _env("HDB_ATHENA_WORKGROUP", "primary")

METADATA_GLUE_DATABASE = _env("HDB_METADATA_DATABASE", GLUE_DATABASE)
METADATA_ATHENA_WORKGROUP = _env("HDB_METADATA_WORKGROUP", ATHENA_WORKGROUP)


SOURCE_S3_BUCKET = _env("HDB_SOURCE_BUCKET", f"{_BUCKET_PREFIX}-source")

SOURCE_S3_PREFIX = _env("HDB_SOURCE_PREFIX", "resale-flat-prices-API-ingestion")

SOURCE_MANUAL_UPLOAD_PREFIX = _env("HDB_SOURCE_MANUAL_UPLOAD_PREFIX", "resale-flat-prices-manual-upload")


RAW_S3_BUCKET = _env("HDB_RAW_BUCKET", f"{_BUCKET_PREFIX}-raw")
CLEANED_S3_BUCKET = _env("HDB_CLEANED_BUCKET", f"{_BUCKET_PREFIX}-cleaned")
TRANSFORMED_S3_BUCKET = _env("HDB_TRANSFORMED_BUCKET", f"{_BUCKET_PREFIX}-transformed")
HASHED_S3_BUCKET = _env("HDB_HASHED_BUCKET", f"{_BUCKET_PREFIX}-hashed")
FAILED_S3_BUCKET = _env("HDB_FAILED_BUCKET", f"{_BUCKET_PREFIX}-failed")
AUDIT_S3_BUCKET = _env("HDB_AUDIT_BUCKET", f"{_BUCKET_PREFIX}-audit-tables")
PIPELINE_SCRIPTS_S3_BUCKET = _env("HDB_SCRIPTS_BUCKET", f"{_BUCKET_PREFIX}-pipeline-scripts")

TABLES = {
    "raw_staging": ("raw_iceberg_staging", f"s3://{RAW_S3_BUCKET}/raw_iceberg_staging/"),
    "raw":         ("raw_iceberg",         f"s3://{RAW_S3_BUCKET}/raw_iceberg/"),
    "cleaned_staging":     ("cleaned_iceberg_staging",     f"s3://{CLEANED_S3_BUCKET}/cleaned_iceberg_staging/"),
    "cleaned":             ("cleaned_iceberg",             f"s3://{CLEANED_S3_BUCKET}/cleaned_iceberg/"),
    "transformed_staging": ("transformed_iceberg_staging", f"s3://{TRANSFORMED_S3_BUCKET}/transformed_iceberg_staging/"),
    "transformed":         ("transformed_iceberg",         f"s3://{TRANSFORMED_S3_BUCKET}/transformed_iceberg/"),
    "hashed_staging":      ("hashed_iceberg_staging",      f"s3://{HASHED_S3_BUCKET}/hashed_iceberg_staging/"),
    "hashed":              ("hashed_iceberg",              f"s3://{HASHED_S3_BUCKET}/hashed_iceberg/"),
    "failed":              ("failed_iceberg",              f"s3://{FAILED_S3_BUCKET}/failed_iceberg/"),
    "audit":               ("audit_iceberg",               f"s3://{AUDIT_S3_BUCKET}/audit_iceberg/"),
}


COLLECTION_API_BASE = _env_or_ssm(
    "HDB_COLLECTION_API_BASE", f"{SSM_PARAMETER_PREFIX}/collection_api_base",
    "https://api-production.data.gov.sg/v2/public/api",
)
DATASET_API_BASE = _env_or_ssm(
    "HDB_DATASET_API_BASE", f"{SSM_PARAMETER_PREFIX}/dataset_api_base",
    "https://api-open.data.gov.sg/v1/public/api",
)

COLLECTION_ID = _env_int("HDB_COLLECTION_ID", 189)
DATE_RANGE_START = _env("HDB_DATE_RANGE_START", "2012-01-01")
DATE_RANGE_END = _env("HDB_DATE_RANGE_END", "2016-12-31")

POLL_INTERVAL_SECONDS = _env_int("HDB_POLL_INTERVAL_SECONDS", 5)
POLL_TIMEOUT_SECONDS = _env_int("HDB_POLL_TIMEOUT_SECONDS", 300)
REQUEST_TIMEOUT_SECONDS = _env_int("HDB_REQUEST_TIMEOUT_SECONDS", 30)

MAX_API_RETRIES = _env_int("HDB_MAX_API_RETRIES", 5)
RETRY_BACKOFF_BASE_SECONDS = _env_int("HDB_RETRY_BACKOFF_BASE_SECONDS", 2)

MAX_CONCURRENT_DOWNLOADS = _env_int("HDB_MAX_CONCURRENT_DOWNLOADS", 3)

MAX_CONCURRENT_S3_READS = _env_int("HDB_MAX_CONCURRENT_S3_READS", 8)

MAX_CONCURRENT_ATHENA_INSERTS = _env_int("HDB_MAX_CONCURRENT_ATHENA_INSERTS", 4)

MAX_DATASETS_TO_INGEST = _env_int("HDB_MAX_DATASETS", 0)
MAX_INGEST_ROWS_PER_DATASET = _env_int("HDB_MAX_INGEST_ROWS_PER_DATASET", 0)
MAX_ROWS_TO_INGEST = _env_int("HDB_MAX_ROWS", 0)


LEASE_YEARS = _env_int("HDB_LEASE_YEARS", 99)

NATURAL_KEY_COLUMNS = _env_list(
    "HDB_NATURAL_KEY_COLUMNS",
    ["month", "town", "flat_type", "block", "street_name",
     "storey_range", "floor_area_sqm", "flat_model", "lease_commence_date"],
)

MIN_CATEGORY_FREQUENCY = _env_int("HDB_MIN_CATEGORY_FREQUENCY", 5)

ATTRIBUTE_COLUMNS_FOR_CHANGE_DETECTION = _env_list(
    "HDB_SCD2_ATTRIBUTE_COLUMNS",
    ["resale_price", "resale_identifier", "remaining_lease_years", "remaining_lease_months"],
)


LOOKBACK_WINDOW_DAYS = _env_int("HDB_LOOKBACK_WINDOW_DAYS", 7)


SNS_TOPIC_NAME = _env("HDB_SNS_TOPIC_NAME", _DEFAULT_SNS_TOPIC_NAME)

SNS_TOPIC_ARN_OVERRIDE = os.environ.get("HDB_SNS_TOPIC_ARN")

ALERT_RECIPIENT_EMAILS = _env_list("HDB_ALERT_RECIPIENT_EMAILS", [])


# Canonical column -> Athena type, for every column name the pipeline is
# EVER expected to write, across every table (raw/cleaned/transformed/
# hashed/failed/audit and their _staging counterparts - a column keeps
# the same meaning and type in every one of them).
#
# BUGFIX (recurring - hit twice now): common.py used to decide each
# column's SQL type by inspecting the CURRENT batch's pandas dtype at
# write time. That's fine on the very first write that creates a table,
# but it's a live landmine afterwards: a free-text field like `block`
# (values like "406" or "406A") reads back as an integer dtype on any
# batch where every value in it happens to be all-digits, and the
# pipeline would then try to INSERT a bare number into what Iceberg
# already has fixed as a STRING column - Athena rejects the whole batch
# with TYPE_MISMATCH, taking down every other (perfectly valid) row in
# the same job run along with it.
#
# This table is the actual fix: a column's type is a property of what
# the column *means*, decided once, here - not re-guessed from whatever
# one batch of data happens to look like. common.py's write path checks
# this FIRST, before ever falling back to the existing Iceberg catalog's
# type or to a fresh per-batch guess (see _canonical_athena_type() in
# common.py) - so a data-dependent fluke like "this run's blocks are all
# numeric" can never again change what type gets written.
#
# Athena types used here (see common.py's _athena_type_for_series /
# _sql_literal for how each is rendered): STRING, BIGINT, DOUBLE,
# BOOLEAN, TIMESTAMP.
COLUMN_TYPES = {
    # --- raw HDB resale-flat source columns (identical meaning in every
    # stage table that carries them through: raw/cleaned/transformed/
    # hashed/failed) ---
    "month": "STRING",
    "town": "STRING",
    "flat_type": "STRING",
    "block": "STRING",
    "street_name": "STRING",
    "storey_range": "STRING",
    "floor_area_sqm": "DOUBLE",
    "flat_model": "STRING",
    "lease_commence_date": "BIGINT",
    "resale_price": "DOUBLE",

    # --- derived by job_3 (cleaned) ---
    "remaining_lease_years": "BIGINT",
    "remaining_lease_months": "BIGINT",
    "surrogate_key": "STRING",
    "_validation_errors": "STRING",

    # --- derived by job_4 (transformed) ---
    "resale_identifier": "STRING",
    "_identifier_valid": "BOOLEAN",

    # --- failed_iceberg tagging columns (route_to_failed) ---
    "_failure_reason": "STRING",
    "_failed_stage": "STRING",
    "_failed_at": "STRING",

    # --- audit_iceberg columns (record_audit) ---
    "run_id": "STRING",
    "job_name": "STRING",
    "stage": "STRING",
    "rows_in": "BIGINT",
    "rows_out": "BIGINT",
    "rows_rejected": "BIGINT",
    "run_timestamp": "STRING",
    "duration_seconds": "DOUBLE",
}
