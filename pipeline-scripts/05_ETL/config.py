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
import sys

import boto3


def _bridge_glue_job_arguments() -> None:
    """AWS Glue Job Arguments arrive as '--KEY VALUE' pairs in sys.argv,
    NOT as OS environment variables - so exporting an HDB_* env var
    locally (e.g. HDB_MAX_ROWS, HDB_PROJECT_NAME) has no effect on a real
    `aws glue start-job-run --arguments '{"--HDB_MAX_ROWS":"1000"}'` run
    unless something bridges the two. This does that bridge: any
    '--HDB_...' argument becomes the equivalent os.environ entry, so every
    _env()/_env_int() call below (and everywhere else in this file) sees
    it exactly the same way it would from a local `export`. Deliberately
    plain sys.argv parsing rather than the `awsglue` library's
    getResolvedOptions() - that library isn't guaranteed present in every
    Glue Python Shell runtime, and this needs zero extra dependency.
    No-op for local runs (sys.argv never has '--HDB_...' entries there),
    and setdefault() means a real env var already set wins if somehow
    both are present.
    """
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
# Project name - drives every resource name below by default
# --------------------------------------------------------------------------- #
# Reads the SAME PROJECT_NAME env var setup.sh/tear_down.sh read (falls back to
# HDB_PROJECT_NAME for a Python-only override), so exporting ONE variable before
# running either setup.sh or the Python pipeline points BOTH at the same
# isolated environment - e.g. a parallel test deployment with a different name:
#     export PROJECT_NAME=hdb-eventdriven-test
#     bash setup.sh                 # provisions mission-hdb-eventdriven-test-<account-id>-*
#     python run_pipeline.py        # now targets the SAME -test- resources
# AWS_REGION is NOT part of this - it's pinned to us-east-1 below and is not
# meant to be overridden (see AWS_REGION's own comment for why: a region
# mismatch between setup.sh and the Python jobs already caused a real
# incident, and an old version of this comment recommending
# `export AWS_REGION=ap-southeast-1` as a test-deployment example is very
# likely how that mismatch happened for real in deploy.yml).
# Every individual HDB_*_BUCKET / HDB_GLUE_DATABASE / HDB_SNS_TOPIC_NAME env
# var below still overrides its own derived default individually, same as
# always - this just changes what the DEFAULT is when you have not set one.
PROJECT_NAME = _env("PROJECT_NAME", _env("HDB_PROJECT_NAME", "hdb-eventdriven"))
_account_id_cache = None


def _get_account_id() -> str:
    """Lazily resolves this AWS account's id live via STS - same call
    `aws sts get-caller-identity` makes, and the same thing
    common.py's get_account_id() does (duplicated here rather than
    imported: common.py imports FROM this file, so importing back the
    other way would be circular). Cached per-process.

    Falls back to a clearly-fake placeholder on any failure (no AWS
    credentials - offline unit tests, deploy.yml's py_compile syntax
    check, local dev before `aws configure`) rather than crashing every
    job at import time - same reasoning as _env_or_ssm() above. Only a
    real run with real credentials needs the real id: it exists purely
    to make the bucket names below globally-unique-safe (S3 bucket names
    are a global namespace - see setup.sh's "BUCKET NAMES" section for
    the real BucketAlreadyExists collision that made this necessary)."""
    global _account_id_cache
    if _account_id_cache is None:
        try:
            _account_id_cache = boto3.client("sts").get_caller_identity()["Account"]
        except Exception:
            _account_id_cache = "000000000000"
    return _account_id_cache


# Must match setup.sh's BUCKET_PREFIX exactly (mission-<project>-<account-id>) -
# see that file's "BUCKET NAMES" section for why the account id is in here.
_BUCKET_PREFIX = f"mission-{PROJECT_NAME}-{_get_account_id()}"
_DEFAULT_GLUE_DATABASE = f"{PROJECT_NAME.replace('-', '_')}_database"
_DEFAULT_SNS_TOPIC_NAME = f"{PROJECT_NAME}-notifications"

# --------------------------------------------------------------------------- #
# AWS region / Glue Catalog / Athena
# --------------------------------------------------------------------------- #

# PINNED to us-east-1, not overridable via env var - matches setup.sh and
# tear_down.sh, which are pinned the same way. This used to be overridable
# (AWS_REGION env var, falling back to HDB_AWS_REGION, falling back to a
# "us-east-1" default) specifically so setup.sh and every Python job would
# always agree on region even if someone customized it - but a mismatch
# STILL happened for real (deploy.yml ended up pointed at ap-southeast-1
# while setup.sh actually provisioned us-east-1), which caused a real run
# of "Iceberg cannot find the requested entity" / "S3 location ... not in
# the same region" failures. Overridable-but-must-agree-everywhere turned
# out to be a trap - pinning it in one place with no override is safer.
AWS_REGION = "us-east-1"

GLUE_DATABASE = _env("HDB_GLUE_DATABASE", _DEFAULT_GLUE_DATABASE)
ATHENA_WORKGROUP = _env("HDB_ATHENA_WORKGROUP", "primary")  # must be engine v3 for Iceberg MERGE/UPDATE

# Metadata DB (setup_metadata.py / metadata_tracking.py) - defaults to the
# same database as the pipeline tables, override separately if you keep
# metadata (watermarks, pipeline_runs) in its own Glue database.
METADATA_GLUE_DATABASE = _env("HDB_METADATA_DATABASE", GLUE_DATABASE)
METADATA_ATHENA_WORKGROUP = _env("HDB_METADATA_WORKGROUP", ATHENA_WORKGROUP)

# --------------------------------------------------------------------------- #
# S3 buckets - matches the real bucket structure (8 buckets)
# --------------------------------------------------------------------------- #

SOURCE_S3_BUCKET = _env("HDB_SOURCE_BUCKET", f"{_BUCKET_PREFIX}-source")

# job_1's automated data.gov.sg pull lands here. Standardized name (both
# prefixes share the "resale-flat-prices-" stem, distinguished by suffix)
# so the two entry points read as a matched pair in the console, not one
# named thing plus one generic afterthought.
SOURCE_S3_PREFIX = _env("HDB_SOURCE_PREFIX", "resale-flat-prices-API-ingestion")

# A SECOND way a file gets into the pipeline besides job_1's automated
# data.gov.sg pull: someone drops a CSV directly under this prefix, in the
# SAME source bucket. job_2 reads BOTH prefixes every run and combines them
# (see job_2_raw_iceberg.py's read_source_files()) - a manually-uploaded
# file is not a special case handled only at ingestion, it is processed by
# every downstream stage (cleaned/transformed/hashed/failed) exactly the
# same as an automated one. Kept as its own prefix (not mixed into
# SOURCE_S3_PREFIX) so an EventBridge rule can watch ONLY this prefix for
# "Object Created" and start the pipeline immediately on a manual upload,
# without also double-firing on every file job_1's own automated run lands
# under SOURCE_S3_PREFIX (that path is already covered by the separate
# job_1-SUCCEEDED trigger - see setup.sh Step 11D).
SOURCE_MANUAL_UPLOAD_PREFIX = _env("HDB_SOURCE_MANUAL_UPLOAD_PREFIX", "resale-flat-prices-manual-upload")

# NOTE (2026-08-25): archiving of processed source files to a separate
# bucket was removed per request. Source files are read in place by job_2
# and simply left there - safe to reprocess since raw_iceberg is a full
# overwrite every run.

RAW_S3_BUCKET = _env("HDB_RAW_BUCKET", f"{_BUCKET_PREFIX}-raw")
CLEANED_S3_BUCKET = _env("HDB_CLEANED_BUCKET", f"{_BUCKET_PREFIX}-cleaned")
TRANSFORMED_S3_BUCKET = _env("HDB_TRANSFORMED_BUCKET", f"{_BUCKET_PREFIX}-transformed")
HASHED_S3_BUCKET = _env("HDB_HASHED_BUCKET", f"{_BUCKET_PREFIX}-hashed")
FAILED_S3_BUCKET = _env("HDB_FAILED_BUCKET", f"{_BUCKET_PREFIX}-failed")
AUDIT_S3_BUCKET = _env("HDB_AUDIT_BUCKET", f"{_BUCKET_PREFIX}-audit-tables")
PIPELINE_SCRIPTS_S3_BUCKET = _env("HDB_SCRIPTS_BUCKET", f"{_BUCKET_PREFIX}-pipeline-scripts")

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

# job_1's data.gov.sg calls can hit 429 Too Many Requests when several
# dataset downloads are initiated back-to-back in one run (seen on a real
# run: 3 overlapping datasets, the 2nd's initiate-download got 429'd
# immediately after the 1st succeeded). Retried with exponential backoff
# (doubling each attempt) - honors the API's own Retry-After header when
# it sends one, falls back to the backoff schedule otherwise. Not a sign
# of a broken pipeline - a public API being asked for several downloads
# in quick succession is expected to rate-limit sometimes.
MAX_API_RETRIES = _env_int("HDB_MAX_API_RETRIES", 5)
RETRY_BACKOFF_BASE_SECONDS = _env_int("HDB_RETRY_BACKOFF_BASE_SECONDS", 2)

# job_1 used to process datasets fully sequentially (one dataset's
# initiate -> poll -> download -> upload finished before the next one's
# even started, plus a 1s courtesy gap). For a collection with several
# datasets, each with its own multi-second-to-multi-minute poll wait,
# that adds up to roughly the SUM of every dataset's time. Processing a
# small batch concurrently instead turns that into roughly the MAX of
# one batch's time, which is the actual fix for "ingestion is slow".
#
# This is deliberately NOT unbounded parallelism: the comment above
# (MAX_API_RETRIES) documents a real 429 seen with just 2 overlapping
# requests, so firing off every dataset's initiate-download at once
# would likely make rate-limiting worse, not better. A small, bounded
# batch size lets several datasets' downloads overlap (the real time
# saver, since most of each dataset's time is spent waiting on
# poll-download, not on CPU work) while the existing 429 retry/backoff
# logic absorbs whatever contention does happen.
MAX_CONCURRENT_DOWNLOADS = _env_int("HDB_MAX_CONCURRENT_DOWNLOADS", 3)

# Same idea as MAX_CONCURRENT_DOWNLOADS above, but for reading files back
# OUT of S3 (job_2's read_csv_files_from_s3, landing-zone -> raw stage) -
# these are our own S3 objects, not a rate-limited public API, so a
# higher default is safe.
MAX_CONCURRENT_S3_READS = _env_int("HDB_MAX_CONCURRENT_S3_READS", 8)

# Bounded concurrency for the Athena INSERT batches that load a dataframe
# into an Iceberg table (common._insert_iceberg_rows). Kept LOW (not high
# like MAX_CONCURRENT_S3_READS) on purpose: Iceberg table commits are
# optimistic-concurrency-controlled - two INSERTs committing to the SAME
# table at the same moment can collide, and one has to lose and retry
# (see _execute_athena_insert_batch()'s retry-on-conflict logic). A small
# concurrency still overlaps each query's multi-second Athena startup/
# planning latency (the real time cost, not the actual data volume) while
# keeping collisions rare and cheap to retry.
MAX_CONCURRENT_ATHENA_INSERTS = _env_int("HDB_MAX_CONCURRENT_ATHENA_INSERTS", 4)

# Testing knob: cap how many datasets job_1 actually downloads/ingests,
# regardless of how many the date range matches. 0 (default) = no cap,
# ingest everything the range matches - the real run. Set
# HDB_MAX_DATASETS=2 for a cheap end-to-end test of the whole pipeline
# (Step Functions, all 6 jobs, alerting) without paying for a full
# historical pull every time you're just testing plumbing, not data
# correctness. Unset it (or set back to 0) before the real ingestion run.
# TESTING-ONLY - remove this env var (and its 2 call sites in job_1) before final deployment.
MAX_DATASETS_TO_INGEST = _env_int("HDB_MAX_DATASETS", 0)

# Testing knob: cap total ROW count once files are combined into
# raw_iceberg (job_2). 0 (default) = no cap - the real run. Setting this
# caps raw_iceberg's row count directly, which means every downstream
# stage (cleaned/transformed/hashed) automatically processes <= this many
# rows too, without needing a separate cap in each job - for a cheap,
# fast smoke test of the whole Step Functions chain where nobody's
# actually checking the data itself, just that every stage ran correctly.
# Unset it (or set back to 0) before a real/graded run.
# TESTING-ONLY - remove this env var (and its call site in job_2) before final deployment.
MAX_ROWS_TO_INGEST = _env_int("HDB_MAX_ROWS", 0)

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

# Minimum occurrences for a categorical value (town/flat_type/flat_model) to
# be considered "real" rather than a likely typo/garbage value - shared by
# job_2b_data_profiling.py (reports which values fall under this, for
# visibility) and job_3_cleaned_iceberg.py's validate_categorical() (which
# actually rejects them). Single source of truth so profiling and
# validation can never quietly disagree about what counts as "rare". See
# job_3's validate_categorical() docstring for the full rationale.
MIN_CATEGORY_FREQUENCY = _env_int("HDB_MIN_CATEGORY_FREQUENCY", 5)

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
SNS_TOPIC_NAME = _env("HDB_SNS_TOPIC_NAME", _DEFAULT_SNS_TOPIC_NAME)

# Optional escape hatch: a full ARN here overrides the dynamically-built one
# entirely - e.g. publishing to a topic in a DIFFERENT account/region than
# this job runs in. Unset (None) in the normal case, which is what makes
# send_alert() build the ARN fresh from this run's own live identity.
SNS_TOPIC_ARN_OVERRIDE = os.environ.get("HDB_SNS_TOPIC_ARN")

# Who actually gets emailed on success/failure - subscribed to SNS_TOPIC_NAME
# by sns_subscription_setup.py (hdb_1/ project root).
# Deliberately NO real email hardcoded here as a default, for the same
# reason the AWS account id isn't hardcoded anywhere either (see
# common.py's get_account_id()): an email address is personal data, and
# this is a public GitHub repo. Empty by default - set HDB_ALERT_RECIPIENT_EMAILS
# (comma-separated for more than one recipient) or pass --email directly to
# the setup script instead.
#
# NOTE: SNS itself is the delivery mechanism (common.py's send_alert()
# publishes to the topic) - subscribing an email here doesn't change how
# alerts are SENT, it just determines who's actually listening. Publishing
# to a topic with nobody subscribed succeeds silently - "Alert sent" in the
# logs does NOT mean anyone received it until this is set up.
ALERT_RECIPIENT_EMAILS = _env_list("HDB_ALERT_RECIPIENT_EMAILS", [])
