#!/bin/bash
export AWS_PROFILE="${AWS_PROFILE:-sujen}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_REGION}"
# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE
# AWS CLI BASED SETUP
# ============================================================

set -Eeuo pipefail

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"

REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ACCOUNT_ID=""

# ============================================================
# RESOURCE NAMES
# ============================================================
# NOTE: BUCKET_PREFIX and every *_BUCKET name are defined further down,
# AFTER the AWS Account ID is resolved (see "AWS ACCOUNT" section below) -
# see that section's comment for why. Everything else that doesn't need
# to be globally unique (Glue database, IAM role names, Glue job names,
# the Step Functions state machine) is still defined here as before.

GLUE_DATABASE="${PROJECT_NAME//-/_}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"
GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"
EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"
STEPFUNCTIONS_ROLE_NAME="${PROJECT_NAME}-stepfunctions-role"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"

# Hardcoded (not derived from PROJECT_NAME) - must match
# orchestration.py's GLUE_JOB_NAMES dict exactly, since that file's
# existing (previously untested) run_mode="glue" path already references
# these exact names.
GLUE_JOB_1="hdb-job-1-ingestion-to-source"
GLUE_JOB_2="hdb-job-2-raw-iceberg"
GLUE_JOB_2B="hdb-job-2b-data-profiling"
GLUE_JOB_3="hdb-job-3-cleaned-iceberg"
GLUE_JOB_4="hdb-job-4-transformed-iceberg"
GLUE_JOB_5="hdb-job-5-hashed-iceberg"


LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

# Where someone can drop a CSV directly into the source bucket, bypassing
# job_1's automated data.gov.sg pull - must match config.py's
# SOURCE_MANUAL_UPLOAD_PREFIX exactly. Standardized alongside job_1's own
# "resale-flat-prices-API-ingestion" prefix (config.py's SOURCE_S3_PREFIX) -
# same stem, distinct suffix, so the two entry points read as a pair.
# The EventBridge rule at Step 11 watches ONLY this prefix in SOURCE_BUCKET.
SOURCE_MANUAL_UPLOAD_PREFIX="${HDB_SOURCE_MANUAL_UPLOAD_PREFIX:-resale-flat-prices-manual-upload}"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"

PIPELINE_SOURCE_DIRECTORY="${SCRIPT_DIR}/pipeline-scripts"
PIPELINE_PREFIX="python-scripts"

LAMBDA_SOURCE="${SCRIPT_DIR}/pipeline-scripts/01_template_creation/lambda-script"
LAMBDA_ZIP="${SCRIPT_DIR}/pipeline-scripts/lambda_function.zip"

GITHUB_ORG_REPO="${GITHUB_ORG_REPO:-Varalaxmiurs/HDB-resleflat-price}"
GITHUB_ORG="${GITHUB_ORG_REPO%%/*}"
GITHUB_REPO_NAME="${GITHUB_ORG_REPO#*/}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"

GITHUB_OIDC_HOST="token.actions.githubusercontent.com"
GITHUB_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"
GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"

# ============================================================
# EXPECTED METADATA TABLES
# ============================================================

EXPECTED_METADATA_TABLES=(
    "metadata_tables"
    "pipeline_runs"
    "table_parameters"
    "table_watermarks"
)

# ============================================================
# ROLLBACK ON FAILURE
# ============================================================
# If any step below fails (set -Eeuo pipefail means ANY non-zero exit
# trips this), automatically tear down whatever THIS run created rather
# than leaving a half-built, half-broken environment behind - that
# half-built state is exactly what caused a real bug: Step 4A failed with
# ICEBERG_MISSING_METADATA because a Glue Catalog table entry survived a
# previous failed/partial run while its underlying S3 data didn't, and
# the next run's "does this table already exist" check trusted the stale
# Glue entry. Rolling back to nothing on every failure means the NEXT run
# always starts from a truly clean slate instead of that kind of stale,
# half-alive state.
#
# Reuses tear_down.sh itself rather than re-implementing deletion logic
# here - one script owns "how to remove everything for this
# PROJECT_NAME/REGION", used both for a deliberate teardown and an
# automatic rollback. HDB_SKIP_CONFIRM=1 bypasses tear_down.sh's own
# interactive "Are you sure?" prompt, since this call is unattended.
#
# Set HDB_ROLLBACK_ON_FAILURE=0 to disable this (e.g. while debugging a
# failing step and you want the partial state left in place to inspect).
ROLLBACK_ON_FAILURE="${HDB_ROLLBACK_ON_FAILURE:-1}"

rollback() {
    echo ""
    echo "============================================================"
    echo "ROLLING BACK - removing everything this run created"
    echo "============================================================"
    echo "PROJECT_NAME : ${PROJECT_NAME}"
    echo "AWS_REGION   : ${REGION}"
    echo ""

    if HDB_SKIP_CONFIRM=1 PROJECT_NAME="${PROJECT_NAME}" AWS_REGION="${REGION}"         AWS_PROFILE="${AWS_PROFILE}" bash "${SCRIPT_DIR}/tear_down.sh"
    then
        echo ""
        echo "============================================================"
        echo "ROLLBACK COMPLETE - environment is clean, nothing left behind."
        echo "============================================================"
    else
        echo ""
        echo "============================================================"
        echo "ROLLBACK FAILED - some resources may still exist."
        echo "Run tear_down.sh by hand to finish cleaning up."
        echo "============================================================"
    fi
}

on_error() {
    local exit_code=$?
    local failed_line="$1"

    echo ""
    echo "============================================================"
    echo "SETUP FAILED at line ${failed_line} (exit code ${exit_code})"
    echo "============================================================"

    if [[ "${ROLLBACK_ON_FAILURE}" == "1" ]]; then
        rollback
    else
        echo "Rollback disabled (HDB_ROLLBACK_ON_FAILURE=0)."
        echo "Resources created so far are left in place for inspection."
    fi

    exit "${exit_code}"
}

trap 'on_error ${LINENO}' ERR

# ============================================================
# HEADER
# ============================================================

echo "============================================================"
echo "HDB EVENT-DRIVEN ICEBERG PIPELINE"
echo "============================================================"
echo ""
echo "Project Directory : ${SCRIPT_DIR}"
echo "AWS Profile       : ${AWS_PROFILE}"
echo "AWS Region        : ${REGION}"
echo ""

# ============================================================
# AWS CLI VALIDATION
# ============================================================

if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: AWS CLI is not installed."
    exit 1
fi

if ! command -v python >/dev/null 2>&1; then
    echo "ERROR: Python is not installed or not available as 'python'."
    exit 1
fi

# ============================================================
# PYTHON DEPENDENCIES
# ============================================================
# Installs everything this project's Python code needs (awswrangler, boto3,
# pandas, requests - see requirements.txt) before any of it is imported
# below (01_metadata_setup.py, this script's own ZIP-building Python, etc.)
# or run later (run_pipeline.py, sns_subscription_setup.py). Keeps a fresh
# checkout to a true "clone + bash setup.sh" experience - no separate
# "remember to pip install first" step for a new machine/reviewer.
REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements.txt"

if [[ -f "${REQUIREMENTS_FILE}" ]]; then
    python -m pip install --quiet -r "${REQUIREMENTS_FILE}"
else
    echo "WARNING: requirements.txt not found at ${REQUIREMENTS_FILE} - skipping."
fi

# ============================================================
# AWS ACCOUNT
# ============================================================

echo "Checking AWS identity..."

ACCOUNT_ID="$(
    aws sts get-caller-identity \
        --profile "${AWS_PROFILE}" \
        --query Account \
        --output text \
        --region "${REGION}"
)"

if [[ -z "${ACCOUNT_ID}" || "${ACCOUNT_ID}" == "None" ]]; then
    echo "ERROR: Unable to determine AWS Account ID."
    exit 1
fi

echo "AWS Account       : ${ACCOUNT_ID}"
echo "AWS Region        : ${REGION}"
echo ""

# ============================================================
# BUCKET NAMES (defined here, not at the top of the file)
# ============================================================
# S3 bucket names are a GLOBAL namespace shared across every AWS account
# on Earth, not scoped to this account - "mission-${PROJECT_NAME}-*" alone
# collided with a bucket some OTHER AWS account already owns
# (BucketAlreadyExists on create-bucket, even though create_bucket() below
# had already confirmed via list-buckets that THIS account doesn't have
# it - list-buckets only sees your own account's buckets, so it can't
# detect someone else's name collision ahead of time). Suffixing every
# bucket name with this account's 12-digit ACCOUNT_ID makes collision
# with anyone else's bucket effectively impossible, since no other AWS
# account can ever share your account ID. This is why these names are
# defined here, after ACCOUNT_ID is resolved above, instead of at the top
# of the file with everything else.
BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"

SOURCE_BUCKET="${BUCKET_PREFIX}-source"
RAW_BUCKET="${BUCKET_PREFIX}-raw"
CLEANED_BUCKET="${BUCKET_PREFIX}-cleaned"
TRANSFORMED_BUCKET="${BUCKET_PREFIX}-transformed"
HASHED_BUCKET="${BUCKET_PREFIX}-hashed"
FAILED_BUCKET="${BUCKET_PREFIX}-failed"
PIPELINE_BUCKET="${BUCKET_PREFIX}-pipeline-scripts"
AUDIT_BUCKET="${BUCKET_PREFIX}-audit-tables"

# ============================================================
# HELPER - CREATE S3 BUCKET
# ============================================================

# BUCKETS_CREATED / BUCKETS_EXISTED count outcomes silently - Step 1
# prints one summary line from these instead of a line per bucket.
BUCKETS_CREATED=0
BUCKETS_EXISTED=0

create_bucket() {

    local BUCKET_NAME="$1"

    EXISTING_BUCKET="$(
        aws s3api list-buckets \
            --profile "${AWS_PROFILE}" \
            --query "Buckets[?Name=='${BUCKET_NAME}'].Name" \
            --output text
    )"

    if [[ -n "${EXISTING_BUCKET}" && "${EXISTING_BUCKET}" != "None" ]]; then

        EXISTING_REGION="$(
            aws s3api get-bucket-location \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --query LocationConstraint \
                --output text 2>/dev/null || true
        )"

        if [[ "${EXISTING_REGION}" == "None" || -z "${EXISTING_REGION}" ]]; then
            EXISTING_REGION="us-east-1"
        fi

        if [[ "${EXISTING_REGION}" != "${REGION}" ]]; then
            echo "ERROR: Bucket ${BUCKET_NAME} already exists in ${EXISTING_REGION},"
            echo "but this deployment requires ${REGION}."
            exit 1
        fi

        BUCKETS_EXISTED=$((BUCKETS_EXISTED + 1))

    else

        if [[ "${REGION}" == "us-east-1" ]]; then

            aws s3api create-bucket \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --region "${REGION}" \
                >/dev/null

        else

            aws s3api create-bucket \
                --profile "${AWS_PROFILE}" \
                --bucket "${BUCKET_NAME}" \
                --region "${REGION}" \
                --create-bucket-configuration \
                    "LocationConstraint=${REGION}" \
                >/dev/null

        fi

        BUCKETS_CREATED=$((BUCKETS_CREATED + 1))

    fi
}

# ============================================================
# STEP 1 - CREATE S3 BUCKETS
# ============================================================

create_bucket "${SOURCE_BUCKET}"
create_bucket "${RAW_BUCKET}"
create_bucket "${CLEANED_BUCKET}"
create_bucket "${TRANSFORMED_BUCKET}"
create_bucket "${HASHED_BUCKET}"
create_bucket "${FAILED_BUCKET}"
create_bucket "${PIPELINE_BUCKET}"
create_bucket "${AUDIT_BUCKET}"

# ============================================================
# STEP 2 - ICEBERG PREFIX MARKERS
# ============================================================

create_prefix() {

    local BUCKET_NAME="$1"
    local KEY="$2"

    aws s3api put-object \
        --profile "${AWS_PROFILE}" \
        --bucket "${BUCKET_NAME}" \
        --key "${KEY}" \
        --region "${REGION}" \
        >/dev/null
}

create_prefix "${RAW_BUCKET}" "raw/"
create_prefix "${CLEANED_BUCKET}" "cleaned/"
create_prefix "${TRANSFORMED_BUCKET}" "transformed/"
create_prefix "${HASHED_BUCKET}" "hashed/"
create_prefix "${FAILED_BUCKET}" "failed/"
create_prefix "${AUDIT_BUCKET}" "audit/"

# Visible, ready-to-use folder for manually uploading a CSV straight into
# the pipeline (see Step 11's EventBridge rule) - job_1's own automated
# prefix needs no such marker since job_1 creates it on first upload.
create_prefix "${SOURCE_BUCKET}" "${SOURCE_MANUAL_UPLOAD_PREFIX}/"

# ============================================================
# STEP 3 - PIPELINE SCRIPT BUCKET
# ============================================================

if [[ ! -d "${PIPELINE_SOURCE_DIRECTORY}" ]]; then

    echo "ERROR: Pipeline scripts directory not found:"
    echo "${PIPELINE_SOURCE_DIRECTORY}"

    exit 1
fi

aws s3api put-object \
    --profile "${AWS_PROFILE}" \
    --bucket "${PIPELINE_BUCKET}" \
    --key "${PIPELINE_PREFIX}/" \
    --region "${REGION}" \
    >/dev/null

aws s3 cp \
    --profile "${AWS_PROFILE}" \
    "${PIPELINE_SOURCE_DIRECTORY}/" \
    "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
    --recursive \
    --quiet \
    --exclude "*__pycache__*" \
    --exclude "*.pyc" \
    --exclude "*.ipynb_checkpoints*" \
    --region "${REGION}"

FILE_COUNT="$(
    aws s3 ls \
        --profile "${AWS_PROFILE}" \
        "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
        --recursive \
        --region "${REGION}" |
        wc -l
)"

# ============================================================
# STEP 4 - GLUE DATABASE
# ============================================================

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then
    :
else

    aws glue create-database \
        --profile "${AWS_PROFILE}" \
        --database-input \
        "Name=${GLUE_DATABASE},Description=HDB Event Driven Iceberg Database" \
        --region "${REGION}" \
        >/dev/null

fi

# ============================================================
# STEP 4A - METADATA TABLE SETUP
# ============================================================

METADATA_SCRIPT="${SCRIPT_DIR}/pipeline-scripts/02_meta_datasetup/01_metadata_setup.py"

if [[ ! -f "${METADATA_SCRIPT}" ]]; then

    echo "ERROR: Metadata setup script not found:"
    echo "${METADATA_SCRIPT}"

    exit 1
fi

python "${METADATA_SCRIPT}" \
    --database "${GLUE_DATABASE}" \
    --workgroup "${ATHENA_WORKGROUP}" \
    --metadata-bucket "${AUDIT_BUCKET}" \
    --region "${REGION}" \
    >/dev/null

# ============================================================
# STEP 5 - LAMBDA IAM ROLE
# ============================================================

LAMBDA_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"lambda.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --assume-role-policy-document "${LAMBDA_TRUST_POLICY}" \
        >/dev/null

    sleep 10
fi

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AmazonAthenaFullAccess

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

LAMBDA_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

# ============================================================
# STEP 6 - GLUE IAM ROLE
# ============================================================

GLUE_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"glue.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --assume-role-policy-document "${GLUE_TRUST_POLICY}" \
        >/dev/null

fi

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AmazonS3FullAccess

# Athena permission is needed here (not just on the Lambda role) because
# every job_*.py's actual Iceberg reads/writes go through awswrangler's
# Athena-backed engine (common.py's athena_read_sql/execute_athena_sql) -
# a real Glue Job run under this role would fail without it.
aws iam attach-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AmazonAthenaFullAccess

GLUE_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

# ============================================================
# STEP 6B - GLUE JOBS (one per pipeline stage)
# ============================================================
# Registers the 6 Glue Python Shell jobs that let this pipeline actually
# run on AWS (via the Step Functions state machine below) instead of
# only locally. Every job_*.py only imports common.py/config.py locally
# (verified - no other cross-file dependency across job_1..job_5), so
# --extra-py-files just needs those two uploaded alongside each script.
# Third-party packages (awswrangler/pandas/requests) install at job
# start via --additional-python-modules - boto3 is already built in.
#
# awswrangler is left as a BARE FLOOR ("awswrangler>=3.9.0") and pip is
# told to install WHEELS ONLY (below). Four real bugs hit chasing this
# on 2026-08-24, in order:
#   1. Left fully bare ("awswrangler,pandas,requests"): resolved an old
#      release predating wr.athena.to_iceberg() entirely. job_1 ran
#      fine (downloaded + processed data) but failed on its own
#      closing record_audit() call: "AttributeError: module
#      'awswrangler.athena' has no attribute 'to_iceberg'".
#   2. Pinned to a bare floor ("awswrangler>=3.17.1"): confirmed via
#      the real Glue job's own CloudWatch log that awswrangler 3.15.0+
#      all declare Requires-Python >=3.10, but Glue Python Shell only
#      ever offers PythonVersion 2.7/3.6/3.9 (this job runs 3.9 - no
#      3.10+ option exists for the Python Shell job type, only Glue's
#      Spark/ETL job type has that) - every 3.15+ release gets filtered
#      out, and with nothing below 3.15 allowed either (floor was too
#      high), pip had no version left: "No matching distribution found
#      for awswrangler>=3.17.1".
#   3. Pinned to a RANGE ("awswrangler>=3.9.0,<3.15.0"): also failed.
#      Confirmed via CloudWatch ("ERROR: Invalid requirement: '<3.15.0'")
#      that Glue's own --additional-python-modules parser splits this
#      whole argument on every comma to build its install list - which
#      also splits the comma INSIDE that one range spec, handing pip a
#      bare "<3.15.0" as if it were its own package (no name, just an
#      operator).
#   4. Pinned to an EXACT release ("awswrangler==3.14.0", the newest
#      Python-3.9-compatible one, no internal comma so immune to bug
#      #3): still failed. Confirmed via CloudWatch that awswrangler
#      3.14.0 hard-requires pyarrow==21.0.0 exactly, and pyarrow
#      21.0.0 needs CMake >=3.25 to build from source - which this
#      sandboxed Python Shell environment doesn't have - so pip tried
#      to compile pyarrow from source and failed: "error: command
#      'cmake' failed: No such file or directory". Every further exact
#      pin just drags in whatever pyarrow THAT release wants, so
#      guessing individual version numbers is a dead end.
# SUPERSEDED (2026-08-25): every attempt above still hit the same
# cmake/pyarrow wall eventually - so awswrangler was dropped ENTIRELY.
# common.py's Iceberg read/write layer was rewritten to raw Athena SQL via
# boto3 (no more to_iceberg()/read_sql_query()/wr.s3.read_csv() anywhere in
# pipeline-scripts/05_ETL/*.py - verified), so awswrangler (and its
# cmake-requiring pyarrow dependency) is no longer imported by any job at
# all. Only pandas/requests remain, both of which have prebuilt Python 3.9
# wheels, so the "--only-binary=:all:" installer option is no longer
# needed either. See update_glue_python_modules.py for the equivalent
# targeted fix against already-deployed jobs.
GLUE_ADDITIONAL_PYTHON_MODULES="pandas,requests"
GLUE_PYTHON_MODULES_INSTALLER_OPTION=""

GLUE_TEMP_DIR="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/glue-temp/"
GLUE_EXTRA_PY_FILES="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/common.py,s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/config.py"

create_or_update_glue_job() {

    local JOB_NAME="$1"
    local SCRIPT_FILE="$2"
    local SCRIPT_LOCATION="s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/05_ETL/${SCRIPT_FILE}"

    local DEFAULT_ARGUMENTS
    DEFAULT_ARGUMENTS="$(cat <<JSONEOF
{
  "--extra-py-files": "${GLUE_EXTRA_PY_FILES}",
  "--additional-python-modules": "${GLUE_ADDITIONAL_PYTHON_MODULES}",
  "--TempDir": "${GLUE_TEMP_DIR}"
}
JSONEOF
)"
    # "--python-modules-installer-option" (--only-binary=:all:) deliberately
    # dropped (2026-08-25): only needed to force wheel-only installs of
    # awswrangler/pyarrow, which are no longer installed at all - see
    # GLUE_PYTHON_MODULES_INSTALLER_OPTION's definition above. Left as an
    # empty DefaultArguments value instead of omitting the key entirely was
    # considered and rejected, matching update_glue_python_modules.py's own
    # choice to drop the key rather than leave a stale/empty one behind.
    if [[ -n "${GLUE_PYTHON_MODULES_INSTALLER_OPTION}" ]]; then
        DEFAULT_ARGUMENTS="$(
            python3 -c "
import json, sys
args = json.loads(sys.argv[1])
args['--python-modules-installer-option'] = sys.argv[2]
print(json.dumps(args))
" "${DEFAULT_ARGUMENTS}" "${GLUE_PYTHON_MODULES_INSTALLER_OPTION}"
        )"
    fi

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${JOB_NAME}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then

        local JOB_UPDATE
        JOB_UPDATE="$(cat <<JSONEOF
{
  "Role": "${GLUE_ROLE_ARN}",
  "Command": {"Name": "pythonshell", "ScriptLocation": "${SCRIPT_LOCATION}", "PythonVersion": "3.9"},
  "DefaultArguments": ${DEFAULT_ARGUMENTS},
  "MaxCapacity": 1,
  "Timeout": 30
}
JSONEOF
)"

        aws glue update-job \
            --profile "${AWS_PROFILE}" \
            --job-name "${JOB_NAME}" \
            --job-update "${JOB_UPDATE}" \
            --region "${REGION}" \
            >/dev/null

    else

        aws glue create-job \
            --profile "${AWS_PROFILE}" \
            --name "${JOB_NAME}" \
            --role "${GLUE_ROLE_ARN}" \
            --command "Name=pythonshell,ScriptLocation=${SCRIPT_LOCATION},PythonVersion=3.9" \
            --default-arguments "${DEFAULT_ARGUMENTS}" \
            --max-capacity 1 \
            --timeout 30 \
            --region "${REGION}" \
            >/dev/null

    fi
}

create_or_update_glue_job "${GLUE_JOB_1}"  "job_1_ingestion_to_source.py"
create_or_update_glue_job "${GLUE_JOB_2}"  "job_2_raw_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_2B}" "job_2b_data_profiling.py"
create_or_update_glue_job "${GLUE_JOB_3}"  "job_3_cleaned_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_4}"  "job_4_transformed_iceberg.py"
create_or_update_glue_job "${GLUE_JOB_5}"  "job_5_hashed_iceberg.py"

# ============================================================
# STEP 7 - EVENTBRIDGE IAM ROLE
# ============================================================

EVENT_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"events.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --assume-role-policy-document "${EVENT_TRUST_POLICY}" \
        >/dev/null

fi

# ============================================================
# STEP 8 - SNS
# ============================================================

SNS_TOPIC_ARN="$(
    aws sns create-topic \
        --profile "${AWS_PROFILE}" \
        --name "${SNS_TOPIC_NAME}" \
        --region "${REGION}" \
        --query TopicArn \
        --output text
)"

# ============================================================
# STEP 8B - SNS EMAIL SUBSCRIPTION (optional)
# ============================================================
# Only runs when HDB_ALERT_RECIPIENT_EMAILS is set BEFORE calling this
# script - e.g. `export HDB_ALERT_RECIPIENT_EMAILS=you@example.com` then
# `bash setup.sh`. Deliberately skipped (not defaulted to some placeholder)
# otherwise - same reasoning as the account id never being hardcoded: an
# email address is personal data, and this is a public GitHub repo, so
# there is no safe default to fall back to here. sns_subscription_setup.py
# reads this same env var itself (via config.ALERT_RECIPIENT_EMAILS), so
# this call passes no --email flag - it just triggers the same script your
# own env var already configures.
if [[ -n "${HDB_ALERT_RECIPIENT_EMAILS:-}" ]]; then
    python "${SCRIPT_DIR}/sns_subscription_setup.py" --region "${REGION}"
else
    echo "Skipped: HDB_ALERT_RECIPIENT_EMAILS not set."
    echo "Run this later to subscribe an email for alerts:"
    echo "  python ${SCRIPT_DIR}/sns_subscription_setup.py --region ${REGION} --email you@example.com"
fi

# ============================================================
# STEP 9 - GITHUB ACTIONS OIDC ROLE
# ============================================================

OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${GITHUB_OIDC_HOST}"

if aws iam list-open-id-connect-providers \
    --profile "${AWS_PROFILE}" \
    --query "OpenIDConnectProviderList[?Arn=='${OIDC_PROVIDER_ARN}'].Arn" \
    --output text |
    grep -q .
then
    :
else

    aws iam create-open-id-connect-provider \
        --profile "${AWS_PROFILE}" \
        --url "https://${GITHUB_OIDC_HOST}" \
        --client-id-list sts.amazonaws.com \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}" \
        >/dev/null

fi

GHA_TRUST_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "${OIDC_PROVIDER_ARN}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${GITHUB_OIDC_HOST}:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "${GITHUB_OIDC_HOST}:sub": "repo:${GITHUB_ORG}*/${GITHUB_REPO_NAME}*:ref:refs/heads/${GITHUB_BRANCH}"
        }
      }
    }
  ]
}
JSON
)"

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    >/dev/null 2>&1
then
    aws iam update-assume-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --assume-role-policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

fi

GHA_S3_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${PIPELINE_BUCKET}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${PIPELINE_BUCKET}/*"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "GitHubActionsS3PipelineAccess" \
    --policy-document "${GHA_S3_POLICY}" \
    >/dev/null

GHA_GLUE_SNS_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJob"
      ],
      "Resource": "arn:aws:glue:${REGION}:${ACCOUNT_ID}:job/hdb-job-*"
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-github-actions-glue-sns" \
    --policy-document "${GHA_GLUE_SNS_POLICY}" \
    >/dev/null

# Lets CI/CD (deploy.yml) push an updated state machine definition on every
# push to main, same "git is the source of truth, CI keeps AWS in sync"
# principle it already applies to the Glue scripts via s3 sync above.
GHA_STEPFUNCTIONS_POLICY="$(
    cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "states:DescribeStateMachine",
        "states:UpdateStateMachine"
      ],
      "Resource": "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-github-actions-stepfunctions" \
    --policy-document "${GHA_STEPFUNCTIONS_POLICY}" \
    >/dev/null

GHA_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

# ============================================================
# STEP 10 - LAMBDA PACKAGE + DEPLOY
# ============================================================

if [[ ! -d "${LAMBDA_SOURCE}" ]]; then

    echo "ERROR: Lambda source directory not found:"
    echo "${LAMBDA_SOURCE}"

    exit 1
fi

if [[ ! -f "${LAMBDA_SOURCE}/lambda_function.py" ]]; then

    echo "ERROR: lambda_function.py not found:"
    echo "${LAMBDA_SOURCE}/lambda_function.py"

    exit 1
fi

find "${LAMBDA_SOURCE}" \
    -type f \
    ! -path "*/__pycache__/*" \
    ! -name "*.pyc" \
    ! -name ".DS_Store" \
    >/dev/null

rm -f "${LAMBDA_ZIP}"

python - "${LAMBDA_SOURCE}" "${LAMBDA_ZIP}" <<'PYEOF'
import sys
import zipfile
from pathlib import Path

source_dir = Path(sys.argv[1])
zip_path = Path(sys.argv[2])

with zipfile.ZipFile(
    zip_path,
    "w",
    zipfile.ZIP_DEFLATED
) as zf:

    for path in sorted(source_dir.rglob("*")):

        if not path.is_file():
            continue

        relative_path = path.relative_to(source_dir)

        if "__pycache__" in relative_path.parts:
            continue

        if path.suffix == ".pyc":
            continue

        if path.name == ".DS_Store":
            continue

        zf.write(path, relative_path)

PYEOF

if [[ ! -s "${LAMBDA_ZIP}" ]]; then
    echo "ERROR: Lambda ZIP was not created."
    exit 1
fi

# Git Bash represents this path as /c/Users/... (POSIX-style), which
# aws.exe (a native Windows binary) cannot resolve when it is embedded
# inside a fileb:// URI - MSYS's usual automatic path translation only
# rewrites bare path-like arguments, not paths embedded inside another
# string, so the CLI receives the literal (invalid on Windows) path and
# fails with "No such file or directory" even though the file exists.
# cygpath -w converts it to a real Windows path (C:\Users\...) first -
# same fix test_step10.sh already applies for the same reason.
if command -v cygpath >/dev/null 2>&1; then
    LAMBDA_ZIP_FOR_CLI="$(cygpath -w "${LAMBDA_ZIP}")"
else
    LAMBDA_ZIP_FOR_CLI="${LAMBDA_ZIP}"
fi

# ============================================================
# DEPLOY LAMBDA
# ============================================================

if aws lambda get-function \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    >/dev/null 2>&1
then

    aws lambda update-function-code \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP_FOR_CLI}" \
        >/dev/null

    aws lambda update-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --handler lambda_function.lambda_handler \
        --role "${LAMBDA_ROLE_ARN}" \
        --timeout 60 \
        --memory-size 512 \
        --environment \
        "Variables={AWS_REGION_NAME=${REGION},GLUE_DATABASE=${GLUE_DATABASE},ATHENA_WORKGROUP=${ATHENA_WORKGROUP},AUDIT_BUCKET=${AUDIT_BUCKET}}" \
        >/dev/null

else

    aws lambda create-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --role "${LAMBDA_ROLE_ARN}" \
        --handler lambda_function.lambda_handler \
        --timeout 60 \
        --memory-size 512 \
        --zip-file "fileb://${LAMBDA_ZIP_FOR_CLI}" \
        --environment \
        "Variables={AWS_REGION_NAME=${REGION},GLUE_DATABASE=${GLUE_DATABASE},ATHENA_WORKGROUP=${ATHENA_WORKGROUP},AUDIT_BUCKET=${AUDIT_BUCKET}}" \
        >/dev/null

fi

aws lambda wait function-active-v2 \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}"

# Resolved once here, reused wherever the metadata-reader Lambda's ARN is
# needed below: the Step Functions role's invoke permission, the state
# machine definition itself (ReadContextBefore/After, WriteContext,
# WriteFailedRun_*), and the final verification step.
LAMBDA_ARN="$(
    aws lambda get-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Configuration.FunctionArn \
        --output text
)"

# ============================================================
# STEP 11 - MANUAL-UPLOAD TRIGGER RULE (rule only - target wired in 11D,
# once STATE_MACHINE_ARN and EVENT_ROLE_ARN both exist)
# ============================================================
# A file can enter this pipeline two ways: job_1's automated data.gov.sg
# pull (lands under SOURCE_S3_PREFIX), or a person dropping a CSV directly
# under SOURCE_MANUAL_UPLOAD_PREFIX in the SAME source bucket. This rule is
# what makes the manual path event-driven too, instead of needing someone
# to also remember to kick off the pipeline by hand after uploading:
# S3 -> EventBridge "Object Created" -> starts the same Step Functions
# chain job_1's own completion starts (Step 11D).
#
# Scoped to bucket=SOURCE_BUCKET AND key prefix=SOURCE_MANUAL_UPLOAD_PREFIX
# specifically - not "every object created anywhere" (the previous version
# of this rule had no bucket/prefix filter at all, so it fired for
# unrelated S3 activity account-wide and did nothing useful with it) and
# NOT the automated SOURCE_S3_PREFIX (that would double-trigger the state
# machine once per file job_1 lands, on top of the one job_1-SUCCEEDED
# trigger in 11D that already covers the whole automated run in one shot).
#
# S3 only emits events to EventBridge once you turn that on per bucket -
# off by default - hence the put-bucket-notification-configuration call.

aws s3api put-bucket-notification-configuration \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --bucket "${SOURCE_BUCKET}" \
    --notification-configuration '{"EventBridgeConfiguration": {}}' \
    >/dev/null

EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {"name": ["${SOURCE_BUCKET}"]},
    "object": {"key": [{"prefix": "${SOURCE_MANUAL_UPLOAD_PREFIX}/"}]}
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    --event-pattern "${EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

# ============================================================
# STEP 11B - STEP FUNCTIONS IAM ROLE
# ============================================================

STEPFUNCTIONS_TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{
        "Service":"states.amazonaws.com"
      },
      "Action":"sts:AssumeRole"
    }
  ]
}'

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --assume-role-policy-document "${STEPFUNCTIONS_TRUST_POLICY}" \
        >/dev/null

fi

# No single AWS-managed policy covers this role's actual needs (start/poll
# the 6 Glue jobs, manage the EventBridge rule the .sync integration uses
# under the hood to wait for each Glue job to finish, invoke the metadata
# Lambda, publish to SNS) - inline policy, same pattern Step 9's GitHub
# Actions role already uses for its own bespoke permissions.
STEPFUNCTIONS_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
      "Resource": "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/StepFunctionsGetEventForGlueJobRule"
    },
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "${LAMBDA_ARN}"
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${SNS_TOPIC_ARN}"
    }
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    --policy-name "${STEPFUNCTIONS_ROLE_NAME}-policy" \
    --policy-document "${STEPFUNCTIONS_POLICY}" \
    >/dev/null

STEPFUNCTIONS_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

# ============================================================
# STEP 11C - STEP FUNCTIONS STATE MACHINE
# ============================================================
# One state machine, one state per pipeline layer FROM raw_iceberg
# ONWARD (job2 -> job2b -> job3 -> job4 -> job5) - ingestion (job1) is
# deliberately excluded, see Step 11D right after this: it runs
# standalone and its own completion event is what starts this state
# machine. Each state uses the glue:startJobRun.sync integration so Step
# Functions actually WAITS for that Glue job to finish before moving on,
# rather than firing and moving to the next state immediately. Any job's
# failure routes to that job's own NotifyFailure state (same SNS
# topic/subject as success, just a "- FAILURE" suffix, reason in the
# body) and stops there - nothing after it runs. A before/after read of
# the real metadata tables (via the same metadata-reader Lambda
# create_context() already uses locally) brackets the whole run, proving
# it actually changed something.
#
# The ASL JSON itself is built by a separate script
# (pipeline-scripts/05_ETL/build_state_machine_definition.py), not
# inline here, so the CI/CD workflow can regenerate + push the same
# definition without duplicating this logic.

STATE_MACHINE_DEFINITION_FILE="${SCRIPT_DIR}/.state_machine_definition.json"

BUILD_STATE_MACHINE_SCRIPT="${SCRIPT_DIR}/pipeline-scripts/05_ETL/build_state_machine_definition.py"

python "${BUILD_STATE_MACHINE_SCRIPT}" \
    "${SNS_TOPIC_ARN}" \
    "${LAMBDA_ARN}" \
    "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}" \
    "${STATE_MACHINE_DEFINITION_FILE}"

if [[ ! -s "${STATE_MACHINE_DEFINITION_FILE}" ]]; then
    echo "ERROR: State machine definition was not generated."
    exit 1
fi

# Same Git-Bash-on-Windows path-translation fix already applied to the
# Lambda ZIP in Step 10 - aws.exe cannot resolve a POSIX-style path
# embedded inside a file:// URI.
if command -v cygpath >/dev/null 2>&1; then
    STATE_MACHINE_DEFINITION_FOR_CLI="$(cygpath -w "${STATE_MACHINE_DEFINITION_FILE}")"
else
    STATE_MACHINE_DEFINITION_FOR_CLI="${STATE_MACHINE_DEFINITION_FILE}"
fi

STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    >/dev/null 2>&1
then

    aws stepfunctions update-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --definition "file://${STATE_MACHINE_DEFINITION_FOR_CLI}" \
        --role-arn "${STEPFUNCTIONS_ROLE_ARN}" \
        >/dev/null

else

    aws stepfunctions create-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --name "${STATE_MACHINE_NAME}" \
        --definition "file://${STATE_MACHINE_DEFINITION_FOR_CLI}" \
        --role-arn "${STEPFUNCTIONS_ROLE_ARN}" \
        --type STANDARD \
        >/dev/null

fi

rm -f "${STATE_MACHINE_DEFINITION_FILE}" 2>/dev/null || true

# ============================================================
# STEP 11D - INGESTION-COMPLETE TRIGGER RULE
# ============================================================
# Ingestion (job1) runs standalone - manually via
# `aws glue start-job-run --job-name hdb-job-1-ingestion-to-source`, or on
# whatever schedule you add later. This rule is what makes the rest of the
# pipeline automatic: AWS Glue emits a "Glue Job State Change" event the
# moment a job finishes, so a rule filtered on jobName=job1 AND
# state=SUCCEEDED, targeting this state machine, means the Step Functions
# chain (raw_iceberg -> ... -> hashed_iceberg) starts itself the instant
# ingestion's real output is sitting in S3 - no polling, no manual step 2.

INGESTION_TRIGGER_RULE_NAME="${PROJECT_NAME}-ingestion-complete-trigger"

INGESTION_EVENT_PATTERN="$(cat <<JSONEOF
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"],
  "detail": {
    "jobName": ["${GLUE_JOB_1}"],
    "state": ["SUCCEEDED"]
  }
}
JSONEOF
)"

aws events put-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    --event-pattern "${INGESTION_EVENT_PATTERN}" \
    --state ENABLED \
    >/dev/null

# EventBridge starting a Step Functions execution (unlike Lambda) needs an
# IAM role it assumes, not a resource-based permission - reusing
# EVENT_ROLE_NAME (already created in Step 7) rather than a second
# EventBridge role, since its only job either way is "let EventBridge
# start things."
EVENT_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

EVENT_ROLE_SFN_POLICY="$(cat <<JSONEOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "${STATE_MACHINE_ARN}"
    }
  ]
}
JSONEOF
)"

aws iam put-role-policy \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    --policy-name "${EVENT_ROLE_NAME}-start-execution" \
    --policy-document "${EVENT_ROLE_SFN_POLICY}" \
    >/dev/null

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${INGESTION_TRIGGER_RULE_NAME}" \
    --targets "Id=start-pipeline-state-machine,Arn=${STATE_MACHINE_ARN},RoleArn=${EVENT_ROLE_ARN}" \
    >/dev/null

# ============================================================
# STEP 11 (continued) - WIRE THE MANUAL-UPLOAD RULE TO THE SAME STATE
# MACHINE, now that STATE_MACHINE_ARN and EVENT_ROLE_ARN both exist.
# EVENT_ROLE_ARN already has states:StartExecution on STATE_MACHINE_ARN
# from the policy attached just above - no separate role/policy needed,
# EventBridge just needs a second rule pointed at the same target.
# ============================================================

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${EVENT_RULE_NAME}" \
    --targets "Id=start-pipeline-state-machine,Arn=${STATE_MACHINE_ARN},RoleArn=${EVENT_ROLE_ARN}" \
    >/dev/null

# ============================================================
# STEP 12 - VERIFY
# ============================================================

EXPECTED_BUCKETS=(
    "${SOURCE_BUCKET}"
    "${RAW_BUCKET}"
    "${CLEANED_BUCKET}"
    "${TRANSFORMED_BUCKET}"
    "${HASHED_BUCKET}"
    "${FAILED_BUCKET}"
    "${PIPELINE_BUCKET}"
    "${AUDIT_BUCKET}"
)

for bucket in "${EXPECTED_BUCKETS[@]}"; do

    if aws s3api list-buckets \
        --profile "${AWS_PROFILE}" \
        --query "Buckets[?Name=='${bucket}'].Name" \
        --output text |
        grep -Fxq "${bucket}"
    then
        :  # exists as expected - no per-bucket line, the summary below covers it
    else

        echo "ERROR: Missing bucket: ${bucket}"
        exit 1

    fi

done

# ============================================================
# VERIFY GLUE DATABASE
# ============================================================

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Glue database missing: ${GLUE_DATABASE}"
    exit 1

fi

# ============================================================
# VERIFY GLUE METADATA TABLES
# ============================================================

for table in "${EXPECTED_METADATA_TABLES[@]}"; do

    if aws glue get-table \
        --profile "${AWS_PROFILE}" \
        --database-name "${GLUE_DATABASE}" \
        --name "${table}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then
        :  # exists as expected - no per-table line, the summary below covers it
    else

        echo "ERROR: Missing Glue table: ${table}"
        exit 1

    fi

done

# ============================================================
# VERIFY GLUE JOBS
# ============================================================

for job in "${GLUE_JOB_1}" "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}"; do

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${job}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then
        :  # exists as expected

    else

        echo "ERROR: Missing Glue job: ${job}"
        exit 1

    fi

done

# ============================================================
# VERIFY STEP FUNCTIONS STATE MACHINE
# ============================================================

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Step Functions state machine missing: ${STATE_MACHINE_NAME}"
    exit 1

fi

# ============================================================
# VERIFY INGESTION-COMPLETE TRIGGER RULE
# ============================================================

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: Ingestion-complete trigger rule missing: ${INGESTION_TRIGGER_RULE_NAME}"
    exit 1

fi

# ============================================================
# VERIFY LAMBDA
# ============================================================

LAMBDA_STATE="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query State \
        --output text
)"

if [[ "${LAMBDA_STATE}" == "Active" ]]; then
    :
else

    echo "ERROR: Lambda is not Active."
    echo "Current state: ${LAMBDA_STATE}"
    exit 1

fi

# ============================================================
# VERIFY LAMBDA CONFIGURATION
# ============================================================

LAMBDA_HANDLER="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Handler \
        --output text
)"

LAMBDA_TIMEOUT="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query Timeout \
        --output text
)"

if [[ "${LAMBDA_HANDLER}" != "lambda_function.lambda_handler" ]]; then
    echo "ERROR: Unexpected Lambda handler: ${LAMBDA_HANDLER}"
    exit 1
fi

if [[ "${LAMBDA_TIMEOUT}" -lt 60 ]]; then
    echo "ERROR: Lambda timeout is too low: ${LAMBDA_TIMEOUT}"
    exit 1
fi

# ============================================================
# VERIFY EVENTBRIDGE RULE
# ============================================================

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    >/dev/null 2>&1
then
    :
else

    echo "ERROR: EventBridge rule missing: ${EVENT_RULE_NAME}"
    exit 1

fi

# ============================================================
# VERIFY EVENTBRIDGE TARGET
# ============================================================

TARGET_COUNT="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'length(Targets)' \
        --output text
)"

if [[ "${TARGET_COUNT}" -gt 0 ]]; then
    :
else

    echo "ERROR: EventBridge rule has no targets."
    exit 1

fi

# ============================================================
# VERIFY EVENTBRIDGE TARGET ARN
# ============================================================
# EVENT_RULE_NAME is the manual-upload trigger - it targets the state
# machine directly (same as INGESTION_TRIGGER_RULE_NAME), not the Lambda.

TARGET_ARN="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'Targets[0].Arn' \
        --output text
)"

if [[ "${TARGET_ARN}" == "${STATE_MACHINE_ARN}" ]]; then
    :
else

    echo "ERROR: EventBridge target does not match expected state machine."
    echo "Expected: ${STATE_MACHINE_ARN}"
    echo "Actual  : ${TARGET_ARN}"
    exit 1

fi

# ============================================================
# CLEAN TEMPORARY ZIP
# ============================================================

rm -f "${LAMBDA_ZIP}" 2>/dev/null || true

# ============================================================
# SUCCESS
# ============================================================

echo ""
echo "============================================================"
echo "SETUP COMPLETED SUCCESSFULLY"
echo "============================================================"

echo ""
echo "AWS Account:"
echo "  ${ACCOUNT_ID}"

echo ""
echo "AWS Region:"
echo "  ${REGION}"

echo ""
echo "============================================================"
echo "ALL CHECKS PASSED"
echo "============================================================"