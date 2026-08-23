#!/bin/bash

# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE
# AWS CLI BASED SETUP
# ============================================================

set -Eeuo pipefail

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_NAME="hdb-eventdriven"

REGION="${AWS_REGION:-ap-southeast-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "HDB EVENT-DRIVEN ICEBERG PIPELINE"
echo "============================================================"

echo "Project Directory : ${SCRIPT_DIR}"
echo "AWS Region        : ${REGION}"


# ============================================================
# VALIDATE AWS CLI
# ============================================================

if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: AWS CLI is not installed."
    exit 1
fi


# ============================================================
# AWS ACCOUNT
# ============================================================

ACCOUNT_ID="$(aws sts get-caller-identity \
    --query Account \
    --output text \
    --region "${REGION}")"

if [[ -z "${ACCOUNT_ID}" || "${ACCOUNT_ID}" == "None" ]]; then
    echo "ERROR: Unable to determine AWS Account ID."
    exit 1
fi

echo "AWS Account       : ${ACCOUNT_ID}"
echo "AWS Region        : ${REGION}"


# ============================================================
# BUCKETS
# ============================================================



SOURCE_BUCKET="${PROJECT_NAME}-source"

RAW_BUCKET="${PROJECT_NAME}-raw"

CLEANED_BUCKET="${PROJECT_NAME}-cleaned"

TRANSFORMED_BUCKET="${PROJECT_NAME}-transformed"

HASHED_BUCKET="${PROJECT_NAME}-hashed"

FAILED_BUCKET="${PROJECT_NAME}-failed"

PIPELINE_BUCKET="${PROJECT_NAME}-pipeline-scripts"

AUDIT_BUCKET="${PROJECT_NAME}-audit-tables"


# ============================================================
# CREATE S3 BUCKET
# ============================================================

create_bucket() {

    local BUCKET_NAME="$1"

    echo ""
    echo "Checking bucket: ${BUCKET_NAME}"

    if aws s3api head-bucket \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        >/dev/null 2>&1
    then

        echo "Bucket already exists: ${BUCKET_NAME}"

    else

        echo "Creating bucket: ${BUCKET_NAME}"

        aws s3api create-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${REGION}" \
            --create-bucket-configuration \
                LocationConstraint="${REGION}" \
            >/dev/null

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


# ============================================================
# STEP 2 - ICEBERG TABLE PREFIXES
# ============================================================

echo ""
echo "============================================================"
echo "STEP 2 - ICEBERG TABLE PREFIXES"
echo "============================================================"

# One prefix marker per bucket, matching config.py's TABLES dict exactly.
# This used to create a "raw/bronze/silver/gold" layout - that belongs to
# the OTHER pipeline design under pipeline-scripts/ (04_Source_to_Bronze,
# 05_Bronze_to_Silver, 06_Silver_to_Gold), not this raw_iceberg/
# cleaned_iceberg/transformed_iceberg/hashed_iceberg/failed_iceberg/
# audit_iceberg one (pipeline-scripts/ETL/). awswrangler's to_iceberg()
# creates the real table path on first write regardless of these markers,
# so this step is cosmetic (readable S3 console) rather than load-bearing -
# but it should still point at the names the ETL pipeline actually uses.

create_prefix() {

    local BUCKET_NAME="$1"
    local KEY="$2"

    aws s3api put-object \
        --bucket "${BUCKET_NAME}" \
        --key "${KEY}" \
        --region "${REGION}" \
        >/dev/null
}

create_prefix "${RAW_BUCKET}"         "raw_iceberg/"
create_prefix "${CLEANED_BUCKET}"     "cleaned_iceberg/"
create_prefix "${TRANSFORMED_BUCKET}" "transformed_iceberg/"
create_prefix "${HASHED_BUCKET}"      "hashed_iceberg/"
create_prefix "${FAILED_BUCKET}"      "failed_iceberg/"
create_prefix "${AUDIT_BUCKET}"       "audit_iceberg/"

echo "Iceberg table prefixes created (one per bucket, matching config.py's TABLES)."


# ============================================================
# STEP 3 - PIPELINE SCRIPT BUCKET
# ============================================================

echo ""
echo "============================================================"
echo "STEP 3 - PIPELINE SCRIPT BUCKET"
echo "============================================================"

PIPELINE_SOURCE_DIRECTORY="${SCRIPT_DIR}/pipeline-scripts"

PIPELINE_PREFIX="python-scripts"


if [[ ! -d "${PIPELINE_SOURCE_DIRECTORY}" ]]; then

    echo "ERROR: Pipeline scripts directory not found:"
    echo "${PIPELINE_SOURCE_DIRECTORY}"

    exit 1

fi


# echo "Pipeline source:"
# echo "${PIPELINE_SOURCE_DIRECTORY}"

# echo "Pipeline S3 location:"
# echo "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/"


# ============================================================
# CREATE PIPELINE PREFIX
# ============================================================

aws s3api put-object \
    --bucket "${PIPELINE_BUCKET}" \
    --key "${PIPELINE_PREFIX}/" \
    --region "${REGION}" \
    >/dev/null


# ============================================================
# UPLOAD ALL PIPELINE FILES
# ============================================================

# echo ""
# # echo "Uploading all pipeline scripts..."

aws s3 cp \
    "${PIPELINE_SOURCE_DIRECTORY}/" \
    "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
    --recursive \
    --region "${REGION}"


# echo ""
# echo "Pipeline scripts uploaded successfully."


# ============================================================
# DISPLAY UPLOADED FILES
# ============================================================

# echo ""
# echo "Pipeline files in S3:"

aws s3 ls \
    "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
    --recursive \
    --region "${REGION}"


# ============================================================
# STEP 4 - GLUE DATABASE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 4 - GLUE DATABASE"
echo "============================================================"

GLUE_DATABASE="${PROJECT_NAME//-/_}_database"


if aws glue get-database \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then

    echo "Glue database already exists:"
    echo "${GLUE_DATABASE}"

else

    aws glue create-database \
        --database-input \
        "Name=${GLUE_DATABASE},Description=HDB Event Driven Iceberg Database" \
        --region "${REGION}" \
        >/dev/null

    echo "Glue database created:"
    echo "${GLUE_DATABASE}"

fi
# ============================================================
# STEP 4A - SETUP METADATA TABLES
# ============================================================

# echo ""
# echo "============================================================"
# echo "STEP 5A - SETTING UP METADATA TABLES"
# echo "============================================================"

METADATA_SCRIPT="${SCRIPT_DIR}/pipeline-scripts/02_meta_datasetup/01_metadata_setup.py"

ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"

if [[ ! -f "${METADATA_SCRIPT}" ]]; then

    echo "ERROR: Metadata setup script not found:"
    echo "${METADATA_SCRIPT}"

    exit 1

fi

# echo "Creating metadata tables..."

python "${METADATA_SCRIPT}" \
    --database "${GLUE_DATABASE}" \
    --workgroup "${ATHENA_WORKGROUP}" \
    --metadata-bucket "${AUDIT_BUCKET}" \
    --region "${REGION}"

# echo "Metadata tables setup completed successfully."

# ============================================================
# STEP 5 - LAMBDA IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 5 - LAMBDA IAM ROLE"
echo "============================================================"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

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
    --role-name "${LAMBDA_ROLE_NAME}" \
    >/dev/null 2>&1
then

    echo "Lambda role already exists:"
    echo "${LAMBDA_ROLE_NAME}"

else

    aws iam create-role \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --assume-role-policy-document "${LAMBDA_TRUST_POLICY}" \
        >/dev/null

    echo "Lambda role created:"
    echo "${LAMBDA_ROLE_NAME}"

fi


aws iam attach-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole


aws iam attach-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/AmazonS3FullAccess



# ============================================================
# STEP 6 - GLUE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 6 - GLUE IAM ROLE"
echo "============================================================"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"

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
    --role-name "${GLUE_ROLE_NAME}" \
    >/dev/null 2>&1
then

    echo "Glue role already exists:"
    echo "${GLUE_ROLE_NAME}"

else

    aws iam create-role \
        --role-name "${GLUE_ROLE_NAME}" \
        --assume-role-policy-document "${GLUE_TRUST_POLICY}" \
        >/dev/null

    echo "Glue role created:"
    echo "${GLUE_ROLE_NAME}"

fi


aws iam attach-role-policy \
    --role-name "${GLUE_ROLE_NAME}" \
    --policy-arn \
    arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole


aws iam attach-role-policy \
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

EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

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
    --role-name "${EVENT_ROLE_NAME}" \
    >/dev/null 2>&1
then

    echo "EventBridge role already exists:"
    echo "${EVENT_ROLE_NAME}"

else

    aws iam create-role \
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

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

SNS_TOPIC_ARN="$(aws sns create-topic \
    --name "${SNS_TOPIC_NAME}" \
    --region "${REGION}" \
    --query TopicArn \
    --output text)"

echo "SNS Topic:"
echo "${SNS_TOPIC_ARN}"


# ============================================================
# STEP 9 - GITHUB ACTIONS OIDC ROLE (CI/CD)
# ============================================================

echo ""
echo "============================================================"
echo "STEP 9 - GITHUB ACTIONS OIDC ROLE (CI/CD)"
echo "============================================================"

# Lets GitHub Actions assume an AWS role via short-lived OIDC tokens
# instead of long-lived access keys stored as repo secrets.
GITHUB_ORG_REPO="${GITHUB_ORG_REPO:-Varalaxmiurs/HDB-resleflat-price}"
GITHUB_ORG="${GITHUB_ORG_REPO%%/*}"
GITHUB_REPO_NAME="${GITHUB_ORG_REPO#*/}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"
GITHUB_OIDC_HOST="token.actions.githubusercontent.com"
GITHUB_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"
GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"  # matches the role already created by hand - do not rename without also updating the workflow YAML's role-to-assume

if aws iam list-open-id-connect-providers \
    --query "OpenIDConnectProviderList[?ends_with(Arn, '${GITHUB_OIDC_HOST}')]" \
    --output text \
    | grep -q .
then

    echo "GitHub OIDC provider already exists."

else

    aws iam create-open-id-connect-provider \
        --url "https://${GITHUB_OIDC_HOST}" \
        --client-id-list sts.amazonaws.com \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}" \
        >/dev/null

    echo "GitHub OIDC provider created."

fi

# Wildcards (*) around the org/repo names below are load-bearing, not
# cosmetic: GitHub appends a numeric owner/repo ID to the OIDC token's sub
# claim (e.g. "repo:Org@123/Repo@456:ref:...") once a repo or account has
# ever been renamed, so an old exact-match policy silently stops matching.
# Confirmed via CloudTrail (AssumeRoleWithWebIdentity AccessDenied events)
# on this repo - the wildcard version matches both the plain and ID-suffixed
# forms.
GHA_TRUST_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${GITHUB_OIDC_HOST}"
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
)

if aws iam get-role \
    --role-name "${GHA_ROLE_NAME}" \
    >/dev/null 2>&1
then

    echo "GitHub Actions role already exists:"
    echo "${GHA_ROLE_NAME}"

    # Keep the trust policy in sync with GITHUB_ORG_REPO/GITHUB_BRANCH above,
    # in case the role was created by hand before this step existed.
    aws iam update-assume-role-policy \
        --role-name "${GHA_ROLE_NAME}" \
        --policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

else

    aws iam create-role \
        --role-name "${GHA_ROLE_NAME}" \
        --assume-role-policy-document "${GHA_TRUST_POLICY}" \
        >/dev/null

    echo "GitHub Actions role created:"
    echo "${GHA_ROLE_NAME}"

fi

# Scoped to only what the deploy workflow needs: write the pipeline scripts
# to S3, start/inspect the Glue jobs, and publish to the real SNS topic
# created in STEP 8 (not a hardcoded/guessed ARN).
#
# Split into the two inline policies that are actually attached to the role
# in AWS today. GitHubActionsS3PipelineAccess was applied by hand (via
# put-role-policy) to unblock the "Sync pipeline-scripts/ to S3" step before
# this part of setup.sh had ever been run - s3:ListBucket needs the bucket
# ARN itself as a resource (not just bucket/*), which is what actually fixed
# the AccessDenied error. Keeping the same policy name here means re-running
# setup.sh updates that exact policy in place instead of creating a third,
# differently-named duplicate. Glue/SNS stay in a separate policy since the
# current workflow doesn't touch them yet - add steps that call Glue or
# publish SNS alerts from CI and this policy already covers it.
GHA_S3_POLICY=$(cat <<JSON
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
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${PIPELINE_BUCKET}/*"
    }
  ]
}
JSON
)

aws iam put-role-policy \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "GitHubActionsS3PipelineAccess" \
    --policy-document "${GHA_S3_POLICY}" \
    >/dev/null

GHA_GLUE_SNS_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJob"],
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
)

aws iam put-role-policy \
    --role-name "${GHA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-github-actions-glue-sns" \
    --policy-document "${GHA_GLUE_SNS_POLICY}" \
    >/dev/null

GHA_ROLE_ARN="$(aws iam get-role \
    --role-name "${GHA_ROLE_NAME}" \
    --query Role.Arn \
    --output text)"

echo "GitHub Actions role ARN:"
echo "${GHA_ROLE_ARN}"


# ============================================================
# STEP 10 - LAMBDA PACKAGE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 10 - LAMBDA PACKAGE"
echo "============================================================"

LAMBDA_SOURCE="${SCRIPT_DIR}/pipline-scripts/01_template_creation/lambda-script"

LAMBDA_ZIP="${SCRIPT_DIR}/lambda_function.zip"


if [[ -d "${LAMBDA_SOURCE}" ]]; then

    if ! command -v zip >/dev/null 2>&1; then
        echo "ERROR: zip command is required to package Lambda."
        echo "CloudShell normally provides zip."
        exit 1
    fi

    rm -f "${LAMBDA_ZIP}"

    (
        cd "${LAMBDA_SOURCE}"

        zip -r "${LAMBDA_ZIP}" . \
            -x "*.pyc" \
            -x "__pycache__/*" \
            -x ".DS_Store" \
            >/dev/null
    )

    echo "Lambda package created:"
    echo "${LAMBDA_ZIP}"

else

    echo "WARNING: Lambda source not found:"
    echo "${LAMBDA_SOURCE}"

fi


# ============================================================
# STEP 11 - EVENTBRIDGE RULE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 11 - EVENTBRIDGE RULE"
echo "============================================================"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"


if aws events describe-rule \
    --name "${EVENT_RULE_NAME}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then

    echo "EventBridge rule already exists:"
    echo "${EVENT_RULE_NAME}"

else

    aws events put-rule \
        --name "${EVENT_RULE_NAME}" \
        --event-pattern '{
          "source":["aws.s3"]
        }' \
        --state ENABLED \
        --region "${REGION}" \
        >/dev/null

    echo "EventBridge rule created:"
    echo "${EVENT_RULE_NAME}"

fi


# ============================================================
# STEP 12 - VERIFY
# ============================================================

echo ""
echo "============================================================"
echo "STEP 12 - VERIFYING SETUP"
echo "============================================================"

echo ""
echo "S3 buckets:"

aws s3api list-buckets \
    --query "Buckets[?starts_with(Name,\`${PROJECT_NAME}\`)].Name" \
    --output text


# echo ""
# echo "Pipeline files:"

aws s3 ls \
    "s3://${PIPELINE_BUCKET}/${PIPELINE_PREFIX}/" \
    --recursive \
    --region "${REGION}"


echo ""
echo "Glue database:"

aws glue get-database \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --query "Database.Name" \
    --output text


# ============================================================
# CLEAN TEMPORARY LAMBDA ZIP
# ============================================================

rm -f "${LAMBDA_ZIP}" 2>/dev/null || true


# ============================================================
# SUCCESS
# ============================================================

echo ""
echo "============================================================"
echo "SETUP COMPLETED SUCCESSFULLY"
echo "============================================================"

# echo ""
# echo "AWS Account:"
# echo "${ACCOUNT_ID}"

# echo ""
# echo "AWS Region:"
# echo "${REGION}"

echo ""
echo "Buckets:"

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
echo "SNS:"
echo "  ${SNS_TOPIC_ARN}"

echo ""
echo "GitHub Actions Role (CI/CD):"
echo "  ${GHA_ROLE_ARN}"

echo ""
echo "============================================================"
