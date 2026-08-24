"""
config.py
==========
Single source of truth for every configurable value used across the HDB
pipeline: common.py, metadata_tracking.py, and job_1 .. job_5. Nothing
below should be hardcoded anywhere else - if you find a bucket name, ARN,
table name, or tuning constant hardcoded in another file, it's a bug.

Every setting has a sensible default AND can be overridden via an
environment variable, so dev/staging/prod can use the same code with
different values.

How to override in AWS Glue:
    Job Python Shell / Spark jobs support arbitrary Job Parameters. Any
    parameter you add as `--HDB_SOME_SETTING value` in the Glue Job's
    "Job parameters" is exposed to the script as an environment variable
    named `HDB_SOME_SETTING` automatically - no extra code needed on top
    of what's already here.

How to override locally (Git Bash):
    export HDB_GLUE_DATABASE=my_dev_database
    python job_1_ingestion_to_source.py
"""

import os

import boto3


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


def _env_list(key: str, default: list) -> list:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return [v.strip() for v in raw.split(",") if v.strip()]


# --------------------------------------------------------------------------- #
# AWS Systems Manager Parameter Store - plain, NON-secret runtime config
# --------------------------------------------------------------------------- #
# Used ONLY for values that are genuinely fine to be centrally-managed
# operational config (today: the data.gov.sg API base URLs) - specifically
# NOT for the AWS account id, which is resolved live via STS instead (see
# common.py's get_account_id()) and is never written here or anywhere else,
# and NOT for actual credentials, which belong in Secrets Manager (see
# common.py's get_secret()) if this pipeline ever has any.
#
# Resolution order per value: explicit env var (Glue Job Parameter / local
# override) > SSM Parameter Store (02_parameter_store_setup.py seeds these
# under SSM_PARAMETER_PREFIX) > a safe hardcoded default (today's real
# public API URLs - not sensitive, fine to ship as a fallback for local/
# offline dev or before the setup script has been run).
SSM_PARAMETER_PREFIX = _env("HDB_SSM_PREFIX", "/hdb-pipeline")

_ssm_client = None


def _get_ssm_client():
    """Lazily created and deliberately NOT pinned to a region the way
    common.py's _BOTO3_SESSION is - boto3's own default region resolution
    (the Glue job's execution region / AWS_DEFAULT_REGION / ~/.aws/config)
    is already correct in every environment this runs in, and this file is
    also imported in contexts (e.g. offline unit tests) where forcing a
    client to exist eagerly would be unwanted."""
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
        # No AWS access (offline unit tests), the parameter hasn't been
        # seeded yet (02_parameter_store_setup.py not run), or this role
        # lacks ssm:GetParameter - fall back to the safe default rather
        # than crash every job at import time.
        return default


# --------------------------------------------------------------------------- #
# AWS region / Glue Catalog / Athena
# --------------------------------------------------------------------------- #

# Checks the plain AWS_REGION env var FIRST (same name setup.sh and boto3
# itself look for), falling back to the project-specific HDB_AWS_REGION
# override, then the hardcoded default. setup.sh reads "${AWS_REGION:-...}"
# directly - if this read HDB_AWS_REGION only, a shell that had AWS_REGION
# set (but not HDB_AWS_REGION) would make setup.sh provision everything in
# one region while every Python job assumed a different one. That exact
# split was the root cause of a whole run of "Iceberg cannot find the
# requested entity" / "S3 location ... not in the same region" failures.
AWS_REGION = _env("AWS_REGION", _env("HDB_AWS_REGION", "us-east-1"))

GLUE_DATABASE = _env("HDB_GLUE_DATABASE", "hdb_eventdriven_database")  # matches setup.sh's ${PROJECT_NAME//-/_}_database
ATHENA_WORKGROUP = _env("HDB_ATHENA_WORKGROUP", "primary")  # must be engine v3 for Iceberg MERGE/UPDATE

# Metadata DB (setup_metadata.py / metadata_tracking.py) - defaults to the
# same database as the pipeline tables, override separately if you keep
# metadata (watermarks, pipeline_runs) in its own Glue database.
METADATA_GLUE_DATABASE = _env("HDB_METADATA_DATABASE", GLUE_DATABASE)
METADATA_ATHENA_WORKGROUP = _env("HDB_METADATA_WORKGROUP", ATHENA_WORKGROUP)

# --------------------------------------------------------------------------- #
# S3 buckets - matches the real bucket structure (8 buckets)
# --------------------------------------------------------------------------- #

SOURCE_S3_BUCKET = _env("HDB_SOURCE_BUCKET", "mission-hdb-eventdriven-source")
SOURCE_S3_PREFIX = _env("HDB_SOURCE_PREFIX", "resale-flat-prices")

RAW_S3_BUCKET = _env("HDB_RAW_BUCKET", "mission-hdb-eventdriven-raw")
CLEANED_S3_BUCKET = _env("HDB_CLEANED_BUCKET", "mission-hdb-eventdriven-cleaned")
TRANSFORMED_S3_BUCKET = _env("HDB_TRANSFORMED_BUCKET", "mission-hdb-eventdriven-transformed")
HASHED_S3_BUCKET = _env("HDB_HASHED_BUCKET", "mission-hdb-eventdriven-hashed")
FAILED_S3_BUCKET = _env("HDB_FAILED_BUCKET", "mission-hdb-eventdriven-failed")
AUDIT_S3_BUCKET = _env("HDB_AUDIT_BUCKET", "mission-hdb-eventdriven-audit-tables")
PIPELINE_SCRIPTS_S3_BUCKET = _env("HDB_SCRIPTS_BUCKET", "mission-hdb-eventdriven-pipeline-scripts")

TABLES = {
    "raw":         ("raw_iceberg",         f"s3://{RAW_S3_BUCKET}/raw_iceberg/"),
    "cleaned":     ("cleaned_iceberg",     f"s3://{CLEANED_S3_BUCKET}/cleaned_iceberg/"),
    "transformed": ("transformed_iceberg", f"s3://{TRANSFORMED_S3_BUCKET}/transformed_iceberg/"),
    "hashed":      ("hashed_iceberg",      f"s3://{HASHED_S3_BUCKET}/hashed_iceberg/"),
    "failed":      ("failed_iceberg",      f"s3://{FAILED_S3_BUCKET}/failed_iceberg/"),
    "audit":       ("audit_iceberg",       f"s3://{AUDIT_S3_BUCKET}/audit_iceberg/"),
}

# --------------------------------------------------------------------------- #
# data.gov.sg API (job_1)
# --------------------------------------------------------------------------- #

COLLECTION_API_BASE = _env_or_ssm(
    "HDB_COLLECTION_API_BASE", f"{SSM_PARAMETER_PREFIX}/collection_api_base",
    "https://api-production.data.gov.sg/v2/public/api",
)
DATASET_API_BASE = _env_or_ssm(
    "HDB_DATASET_API_BASE", f"{SSM_PARAMETER_PREFIX}/dataset_api_base",
    "https://api-open.data.gov.sg/v1/public/api",  # TODO: verify
)

COLLECTION_ID = _env_int("HDB_COLLECTION_ID", 189)
DATE_RANGE_START = _env("HDB_DATE_RANGE_START", "2012-01-01")  # ISO 'YYYY-MM-DD'
DATE_RANGE_END = _env("HDB_DATE_RANGE_END", "2016-12-31")

POLL_INTERVAL_SECONDS = _env_int("HDB_POLL_INTERVAL_SECONDS", 5)
POLL_TIMEOUT_SECONDS = _env_int("HDB_POLL_TIMEOUT_SECONDS", 300)
REQUEST_TIMEOUT_SECONDS = _env_int("HDB_REQUEST_TIMEOUT_SECONDS", 30)

# --------------------------------------------------------------------------- #
# Data quality / transformation rules (job_3, job_4, job_5)
# --------------------------------------------------------------------------- #

LEASE_YEARS = _env_int("HDB_LEASE_YEARS", 99)

# Composite/natural key = all columns except resale_price, per the test
# brief's duplicate-handling rule. Basis for the deterministic surrogate key.
NATURAL_KEY_COLUMNS = _env_list(
    "HDB_NATURAL_KEY_COLUMNS",
    ["month", "town", "flat_type", "block", "street_name",
     "storey_range", "floor_area_sqm", "flat_model", "lease_commence_date"],
)

# Which attribute columns count as "a change" for SCD2 versioning in job_5
ATTRIBUTE_COLUMNS_FOR_CHANGE_DETECTION = _env_list(
    "HDB_SCD2_ATTRIBUTE_COLUMNS",
    ["resale_price", "resale_identifier", "remaining_lease_years", "remaining_lease_months"],
)

# --------------------------------------------------------------------------- #
# Incremental load settings
# --------------------------------------------------------------------------- #

# Seeded into table_parameters as 'lookback_window' (01_metadata_setup.py) -
# standard incremental-extraction pattern: when pulling only rows changed
# since the last watermark, re-pull an extra N days BEFORE that watermark
# too, to catch late-arriving/late-updated source records that landed after
# their own true event date. NOT currently applied anywhere - this pipeline
# is entirely load_type='FULL' today (see common.py's overwrite_iceberg()),
# so there's no "since last watermark" query to widen yet. Kept as tracked
# metadata so the value is already in place for whenever/if a table's
# load_type becomes MERGE/INCREMENTAL and needs it.
LOOKBACK_WINDOW_DAYS = _env_int("HDB_LOOKBACK_WINDOW_DAYS", 7)

# --------------------------------------------------------------------------- #
# Alerting
# --------------------------------------------------------------------------- #

# No account id is ever hardcoded or stored here (not even in Parameter
# Store) - common.py's send_alert() builds the real topic ARN at call time
# from AWS_REGION + SNS_TOPIC_NAME (both plain, non-sensitive) plus
# get_account_id(), which resolves the live account id via STS on every
# run. SNS_TOPIC_NAME just has to match the real topic name setup.sh
# creates - it carries no account/region info itself.
SNS_TOPIC_NAME = _env("HDB_SNS_TOPIC_NAME", "hdb-eventdriven-notifications")

# Optional escape hatch: a full ARN here overrides the dynamically-built one
# entirely - e.g. publishing to a topic in a DIFFERENT account/region than
# this job runs in. Unset (None) in the normal case, which is what makes
# send_alert() build the ARN fresh from this run's own live identity.
SNS_TOPIC_ARN_OVERRIDE = os.environ.get("HDB_SNS_TOPIC_ARN")

SES_SENDER_EMAIL = _env("HDB_SES_SENDER_EMAIL", "pipeline-alerts@example.com")  # must be SES-verified
SES_RECIPIENT_EMAILS = _env_list("HDB_SES_RECIPIENT_EMAILS", ["data-team@example.com"])
