#!/bin/bash
export AWS_PROFILE=sujen
export AWS_REGION=ap-south-1
export AWS_DEFAULT_REGION=ap-south-1
# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE
# AWS CLI BASED SETUP
# ============================================================

set -Eeuo pipefail

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_NAME="hdb-eventdriven"
BUCKET_PREFIX="mission-${PROJECT_NAME}"


REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ACCOUNT_ID=""

# ============================================================
# RESOURCE NAMES
# ============================================================

BUCKET_PREFIX="mission-${PROJECT_NAME}"

SOURCE_BUCKET="${BUCKET_PREFIX}-source"
RAW_BUCKET="${BUCKET_PREFIX}-raw"
CLEANED_BUCKET="${BUCKET_PREFIX}-cleaned"
TRANSFORMED_BUCKET="${BUCKET_PREFIX}-transformed"
HASHED_BUCKET="${BUCKET_PREFIX}-hashed"
FAILED_BUCKET="${BUCKET_PREFIX}-failed"
PIPELINE_BUCKET="${BUCKET_PREFIX}-pipeline-scripts"
AUDIT_BUCKET="${BUCKET_PREFIX}-audit-tables"

GLUE_DATABASE="${PROJECT_NAME//-/_}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"
GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"
EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"

PIPELINE_SOURCE_DIRECTORY="${SCRIPT_DIR}/pipeline-scripts"
PIPELINE_PREFIX="python-scripts"

LAMBDA_SOURCE="${SCRIPT_DIR}/pipeline-scripts/01_template_creation/lambda-script"
LAMBDA_ZIP="${SCRIPT_DIR}/pipeline-scripts/lambda_function.zip"

# EventBridge target.
# Change this only if another Lambda should receive the S3 events.
EVENT_TARGET_LAMBDA="${LAMBDA_FUNCTION_NAME}"

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
# HELPER - CREATE S3 BUCKET
# ============================================================

create_bucket() {

    local BUCKET_NAME="$1"

    echo ""
    echo "Checking bucket: ${BUCKET_NAME}"

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

        echo "Bucket already exists: ${BUCKET_NAME}"
        echo "Bucket region       : ${EXISTING_REGION}"

        if [[ "${EXISTING_REGION}" != "${REGION}" ]]; then
            echo "ERROR: Bucket ${BUCKET_NAME} is in ${EXISTING_REGION},"
            echo "but this deployment requires ${REGION}."
            exit 1
        fi

    else

        echo "Creating bucket: ${BUCKET_NAME}"

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

        echo "Created: ${BUCKET_NAME}"

    fi
}

# ============================================================
# STEP 1 - CREATE S3 BUCKETS
# ============================================================

echo ""
echo "============================================================"
echo "STEP 1 - CREATING S3 BUCKETS"
echo "============================================================"

create_bucket "${SOURCE_BUCKET}"
create_bucket "${RAW_BUCKET}"
create_bucket "${CLEANED_BUCKET}"
create_bucket "${TRANSFORMED_BUCKET}"
create_bucket "${HASHED_BUCKET}"
create_bucket "${FAILED_BUCKET}"
create_bucket "${PIPELINE_BUCKET}"
create_bucket "${AUDIT_BUCKET}"

echo ""
echo "STEP 1 completed."

# ============================================================
# STEP 2 - ICEBERG PREFIX MARKERS
# ============================================================

echo ""
echo "============================================================"
echo "STEP 2 - ICEBERG TABLE PREFIXES"
echo "============================================================"

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

echo "Iceberg prefix markers created."

# ============================================================
# STEP 3 - PIPELINE SCRIPT BUCKET
# ============================================================

echo ""
echo "============================================================"
echo "STEP 3 - PIPELINE SCRIPT BUCKET"
echo "============================================================"

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

echo "Uploading pipeline scripts..."

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

echo "Uploaded ${FILE_COUNT} files."

# ============================================================
# STEP 4 - GLUE DATABASE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 4 - GLUE DATABASE"
echo "============================================================"

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then

    echo "Glue database already exists:"
    echo "${GLUE_DATABASE}"

else

    aws glue create-database \
        --profile "${AWS_PROFILE}" \
        --database-input \
        "Name=${GLUE_DATABASE},Description=HDB Event Driven Iceberg Database" \
        --region "${REGION}" \
        >/dev/null

    echo "Glue database created:"
    echo "${GLUE_DATABASE}"

fi

# ============================================================
# STEP 4A - METADATA TABLE SETUP
# ============================================================

echo ""
echo "============================================================"
echo "STEP 4A - METADATA TABLE SETUP"
echo "============================================================"

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
    --region "${REGION}"

echo "Metadata table setup completed."

# ============================================================
# STEP 5 - LAMBDA IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 5 - LAMBDA IAM ROLE"
echo "============================================================"

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

    echo "Lambda role already exists:"
    echo "${LAMBDA_ROLE_NAME}"

else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --assume-role-policy-document "${LAMBDA_TRUST_POLICY}" \
        >/dev/null

    echo "Lambda role created:"
    echo "${LAMBDA_ROLE_NAME}"

    echo "Waiting for IAM role propagation..."
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

echo "Lambda Role ARN:"
echo "${LAMBDA_ROLE_ARN}"

# ============================================================
# STEP 6 - GLUE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 6 - GLUE IAM ROLE"
echo "============================================================"

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

    echo "Glue role already exists:"
    echo "${GLUE_ROLE_NAME}"

else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --assume-role-policy-document "${GLUE_TRUST_POLICY}" \
        >/dev/null

    echo "Glue role created:"
    echo "${GLUE_ROLE_NAME}"

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

# ============================================================
# STEP 7 - EVENTBRIDGE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 7 - EVENTBRIDGE IAM ROLE"
echo "============================================================"

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

    echo "EventBridge role already exists:"
    echo "${EVENT_ROLE_NAME}"

else

    aws iam create-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --assume-role-policy-document "${EVENT_TRUST_POLICY}" \
        >/dev/null

    echo "EventBridge role created:"
    echo "${EVENT_ROLE_NAME}"

fi

# ============================================================
# STEP 8 - SNS
# ============================================================

echo ""
echo "============================================================"
echo "STEP 8 - SNS"
echo "============================================================"

SNS_TOPIC_ARN="$(
    aws sns create-topic \
        --profile "${AWS_PROFILE}" \
        --name "${SNS_TOPIC_NAME}" \
        --region "${REGION}" \
        --query TopicArn \
        --output text
)"

echo "SNS Topic:"
echo "${SNS_TOPIC_ARN}"

# ============================================================
# STEP 9 - GITHUB ACTIONS OIDC ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 9 - GITHUB ACTIONS OIDC ROLE"
echo "============================================================"

OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${GITHUB_OIDC_HOST}"

if aws iam list-open-id-connect-providers \
    --profile "${AWS_PROFILE}" \
    --query "OpenIDConnectProviderList[?Arn=='${OIDC_PROVIDER_ARN}'].Arn" \
    --output text |
    grep -q .
then

    echo "GitHub OIDC provider already exists."

else

    aws iam create-open-id-connect-provider \
        --profile "${AWS_PROFILE}" \
        --url "https://${GITHUB_OIDC_HOST}" \
        --client-id-list sts.amazonaws.com \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}" \
        >/dev/null

    echo "GitHub OIDC provider created."
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

    echo "GitHub Actions role already exists:"
    echo "${GHA_ROLE_NAME}"

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

    echo "GitHub Actions role created:"
    echo "${GHA_ROLE_NAME}"
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

GHA_ROLE_ARN="$(
    aws iam get-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --query Role.Arn \
        --output text
)"

echo "GitHub Actions Role ARN:"
echo "${GHA_ROLE_ARN}"

# ============================================================
# STEP 10 - LAMBDA PACKAGE + DEPLOY
# ============================================================

echo ""
echo "============================================================"
echo "STEP 10 - LAMBDA PACKAGE + DEPLOY"
echo "============================================================"

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

echo "Lambda source:"
echo "${LAMBDA_SOURCE}"

echo ""
echo "Lambda source files:"

find "${LAMBDA_SOURCE}" \
    -type f \
    ! -path "*/__pycache__/*" \
    ! -name "*.pyc" \
    ! -name ".DS_Store"

echo ""
echo "Creating Lambda ZIP..."

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

print("ZIP created successfully.")
PYEOF

if [[ ! -s "${LAMBDA_ZIP}" ]]; then
    echo "ERROR: Lambda ZIP was not created."
    exit 1
fi

echo ""
echo "ZIP contents:"

python - "${LAMBDA_ZIP}" <<'PYEOF'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1], "r") as z:
    for name in z.namelist():
        print(name)
PYEOF

echo ""
echo "ZIP size:"
ls -lh "${LAMBDA_ZIP}"

# ============================================================
# DEPLOY LAMBDA
# ============================================================

echo ""
echo "Checking Lambda function..."

if aws lambda get-function \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    >/dev/null 2>&1
then

    echo "Lambda already exists."
    echo "Updating Lambda code..."

    aws lambda update-function-code \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP}" \
        >/dev/null

    echo "Lambda code updated."

    echo "Updating Lambda configuration..."

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

    echo "Lambda does not exist."
    echo "Creating Lambda..."

    aws lambda create-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --role "${LAMBDA_ROLE_ARN}" \
        --handler lambda_function.lambda_handler \
        --timeout 60 \
        --memory-size 512 \
        --zip-file "fileb://${LAMBDA_ZIP}" \
        --environment \
        "Variables={AWS_REGION_NAME=${REGION},GLUE_DATABASE=${GLUE_DATABASE},ATHENA_WORKGROUP=${ATHENA_WORKGROUP},AUDIT_BUCKET=${AUDIT_BUCKET}}" \
        >/dev/null

    echo "Lambda created."

fi

echo "Waiting for Lambda to become active..."

aws lambda wait function-active-v2 \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}"

echo "Lambda is ACTIVE."

# ============================================================
# STEP 11 - EVENTBRIDGE RULE + TARGET
# ============================================================

echo ""
echo "============================================================"
echo "STEP 11 - EVENTBRIDGE RULE + TARGET"
echo "============================================================"

EVENT_PATTERN='{
  "source": [
    "aws.s3"
  ],
  "detail-type": [
    "Object Created"
  ]
}'

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    >/dev/null 2>&1
then

    echo "EventBridge rule already exists."

    aws events put-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --name "${EVENT_RULE_NAME}" \
        --event-pattern "${EVENT_PATTERN}" \
        --state ENABLED \
        >/dev/null

else

    aws events put-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --name "${EVENT_RULE_NAME}" \
        --event-pattern "${EVENT_PATTERN}" \
        --state ENABLED \
        >/dev/null

    echo "EventBridge rule created."
fi

echo "EventBridge rule:"
echo "${EVENT_RULE_NAME}"

# ============================================================
# EVENTBRIDGE -> LAMBDA PERMISSION
# ============================================================

LAMBDA_ARN="$(
    aws lambda get-function \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${EVENT_TARGET_LAMBDA}" \
        --query Configuration.FunctionArn \
        --output text
)"

STATEMENT_ID="${EVENT_RULE_NAME}-invoke"

echo ""
echo "Configuring Lambda invocation permission..."

if aws lambda get-policy \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --function-name "${EVENT_TARGET_LAMBDA}" \
    --query "Policy" \
    --output text 2>/dev/null |
    grep -q "${STATEMENT_ID}"
then

    echo "Lambda invocation permission already exists."

else

    aws lambda add-permission \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${EVENT_TARGET_LAMBDA}" \
        --statement-id "${STATEMENT_ID}" \
        --action "lambda:InvokeFunction" \
        --principal events.amazonaws.com \
        --source-arn \
        "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${EVENT_RULE_NAME}" \
        >/dev/null

    echo "Lambda invocation permission added."
fi

# ============================================================
# EVENTBRIDGE TARGET
# ============================================================

echo ""
echo "Configuring EventBridge target..."

aws events put-targets \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --rule "${EVENT_RULE_NAME}" \
    --targets "Id=metadata-reader-lambda,Arn=${LAMBDA_ARN},Input='{\"action\":\"read\"}'" \
    >/dev/null

echo "EventBridge target configured."

# ============================================================
# STEP 12 - VERIFY
# ============================================================

echo ""
echo "============================================================"
echo "STEP 12 - VERIFYING SETUP"
echo "============================================================"

# ============================================================
# VERIFY S3 BUCKETS
# ============================================================

echo ""
echo "S3 buckets:"

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

        echo "OK: ${bucket}"

    else

        echo "ERROR: Missing bucket: ${bucket}"
        exit 1

    fi

done

echo "All expected S3 buckets exist."

# ============================================================
# VERIFY GLUE DATABASE
# ============================================================

echo ""
echo "Glue database:"

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then

    echo "OK: ${GLUE_DATABASE}"

else

    echo "ERROR: Glue database missing: ${GLUE_DATABASE}"
    exit 1

fi

# ============================================================
# VERIFY GLUE METADATA TABLES
# ============================================================

echo ""
echo "Glue metadata tables:"

for table in "${EXPECTED_METADATA_TABLES[@]}"; do

    if aws glue get-table \
        --profile "${AWS_PROFILE}" \
        --database-name "${GLUE_DATABASE}" \
        --name "${table}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then

        echo "OK: ${table}"

    else

        echo "ERROR: Missing Glue table: ${table}"
        exit 1

    fi

done

echo "All metadata tables exist."

# ============================================================
# VERIFY LAMBDA
# ============================================================

echo ""
echo "Lambda:"

LAMBDA_STATE="$(
    aws lambda get-function-configuration \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --query State \
        --output text
)"

if [[ "${LAMBDA_STATE}" == "Active" ]]; then

    echo "OK: ${LAMBDA_FUNCTION_NAME}"
    echo "Lambda state: ${LAMBDA_STATE}"

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

echo "Lambda handler: ${LAMBDA_HANDLER}"
echo "Lambda timeout: ${LAMBDA_TIMEOUT}s"

# ============================================================
# VERIFY EVENTBRIDGE RULE
# ============================================================

echo ""
echo "EventBridge rule:"

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --name "${EVENT_RULE_NAME}" \
    >/dev/null 2>&1
then

    echo "OK: ${EVENT_RULE_NAME}"

else

    echo "ERROR: EventBridge rule missing: ${EVENT_RULE_NAME}"
    exit 1

fi

# ============================================================
# VERIFY EVENTBRIDGE TARGET
# ============================================================

echo ""
echo "EventBridge targets:"

TARGET_COUNT="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'length(Targets)' \
        --output text
)"

if [[ "${TARGET_COUNT}" -gt 0 ]]; then

    echo "OK: ${TARGET_COUNT} target(s) configured."

else

    echo "ERROR: EventBridge rule has no targets."
    exit 1

fi

# ============================================================
# VERIFY EVENTBRIDGE TARGET ARN
# ============================================================

TARGET_ARN="$(
    aws events list-targets-by-rule \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --rule "${EVENT_RULE_NAME}" \
        --query 'Targets[0].Arn' \
        --output text
)"

if [[ "${TARGET_ARN}" == "${LAMBDA_ARN}" ]]; then

    echo "OK: EventBridge targets the metadata-reader Lambda."

else

    echo "ERROR: EventBridge target does not match expected Lambda."
    echo "Expected: ${LAMBDA_ARN}"
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
echo "S3 Buckets:"
echo "  ${SOURCE_BUCKET}"
echo "  ${RAW_BUCKET}"
echo "  ${CLEANED_BUCKET}"
echo "  ${TRANSFORMED_BUCKET}"
echo "  ${HASHED_BUCKET}"
echo "  ${FAILED_BUCKET}"
echo "  ${PIPELINE_BUCKET}"
echo "  ${AUDIT_BUCKET}"

echo ""
echo "Pipeline S3:"
echo "  s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/"

echo ""
echo "Glue Database:"
echo "  ${GLUE_DATABASE}"

echo ""
echo "Metadata Tables:"
for table in "${EXPECTED_METADATA_TABLES[@]}"; do
    echo "  ${table}"
done

echo ""
echo "Lambda:"
echo "  ${LAMBDA_FUNCTION_NAME}"

echo ""
echo "EventBridge:"
echo "  ${EVENT_RULE_NAME}"

echo ""
echo "SNS:"
echo "  ${SNS_TOPIC_ARN}"

echo ""
echo "GitHub Actions Role:"
echo "  ${GHA_ROLE_ARN}"

echo ""
echo "============================================================"
echo "ALL CHECKS PASSED"
echo "============================================================"