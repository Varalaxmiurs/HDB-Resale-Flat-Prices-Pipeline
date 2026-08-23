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
# AWS region / Glue Catalog / Athena
# --------------------------------------------------------------------------- #

AWS_REGION = _env("HDB_AWS_REGION", "ap-southeast-1")

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

SOURCE_S3_BUCKET = _env("HDB_SOURCE_BUCKET", "hdb-eventdriven-source")
SOURCE_S3_PREFIX = _env("HDB_SOURCE_PREFIX", "resale-flat-prices")

RAW_S3_BUCKET = _env("HDB_RAW_BUCKET", "hdb-eventdriven-raw")
CLEANED_S3_BUCKET = _env("HDB_CLEANED_BUCKET", "hdb-eventdriven-cleaned")
TRANSFORMED_S3_BUCKET = _env("HDB_TRANSFORMED_BUCKET", "hdb-eventdriven-transformed")
HASHED_S3_BUCKET = _env("HDB_HASHED_BUCKET", "hdb-eventdriven-hashed")
FAILED_S3_BUCKET = _env("HDB_FAILED_BUCKET", "hdb-eventdriven-failed")
AUDIT_S3_BUCKET = _env("HDB_AUDIT_BUCKET", "hdb-eventdriven-audit-tables")
PIPELINE_SCRIPTS_S3_BUCKET = _env("HDB_SCRIPTS_BUCKET", "hdb-eventdriven-pipeline-scripts")

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

COLLECTION_API_BASE = _env("HDB_COLLECTION_API_BASE", "https://api-production.data.gov.sg/v2/public/api")
DATASET_API_BASE = _env("HDB_DATASET_API_BASE", "https://api-open.data.gov.sg/v1/public/api")  # TODO: verify

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
# Alerting
# --------------------------------------------------------------------------- #

SNS_TOPIC_ARN = _env("HDB_SNS_TOPIC_ARN", "arn:aws:sns:ap-southeast-1:544795558120:hdb-eventdriven-notifications")  # matches the real topic setup.sh creates (account 544795558120)

SES_SENDER_EMAIL = _env("HDB_SES_SENDER_EMAIL", "pipeline-alerts@example.com")  # must be SES-verified
SES_RECIPIENT_EMAILS = _env_list("HDB_SES_RECIPIENT_EMAILS", ["data-team@example.com"])
