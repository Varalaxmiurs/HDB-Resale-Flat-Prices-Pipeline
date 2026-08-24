#!/bin/bash

set -Eeuo pipefail

# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE - TEARDOWN
# ============================================================

PROJECT_NAME="hdb-eventdriven"

REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-sujen}"

# ============================================================
# AWS ACCOUNT
# ============================================================

ACCOUNT_ID="$(
    aws sts get-caller-identity \
        --profile "${AWS_PROFILE}" \
        --query Account \
        --output text \
        --region "${REGION}" \
        --no-cli-pager
)"

# ============================================================
# BUCKET NAMING
# ============================================================
#
# SETUP USES:
#
# PROJECT_NAME="hdb-eventdriven"
# BUCKET_PREFIX="mission-${PROJECT_NAME}"
#
# Therefore:
#
# mission-hdb-eventdriven-source
# mission-hdb-eventdriven-raw
# mission-hdb-eventdriven-cleaned
# etc.
#
# ============================================================

BUCKET_PREFIX="mission-${PROJECT_NAME}"

BUCKETS=(
    "${BUCKET_PREFIX}-source"
    "${BUCKET_PREFIX}-raw"
    "${BUCKET_PREFIX}-cleaned"
    "${BUCKET_PREFIX}-transformed"
    "${BUCKET_PREFIX}-hashed"
    "${BUCKET_PREFIX}-failed"
    "${BUCKET_PREFIX}-pipeline-scripts"
    "${BUCKET_PREFIX}-audit-tables"
)

# ============================================================
# RESOURCE NAMES
# ============================================================

GLUE_DATABASE="${PROJECT_NAME}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"

EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

# ============================================================
# HEADER
# ============================================================

echo ""
echo "============================================================"
echo "HDB PIPELINE TEARDOWN"
echo "============================================================"
echo ""

echo "AWS Account : ${ACCOUNT_ID}"
echo "AWS Profile : ${AWS_PROFILE}"
echo "AWS Region  : ${REGION}"
echo ""

echo "The following resources will be DELETED:"
echo ""

echo "S3 Buckets:"
for BUCKET in "${BUCKETS[@]}"; do
    echo "  ${BUCKET}"
done

echo ""

echo "Glue Database:"
echo "  ${GLUE_DATABASE}"

echo ""

echo "Lambda Function:"
echo "  ${LAMBDA_FUNCTION_NAME}"

echo ""

echo "IAM Roles:"
echo "  ${LAMBDA_ROLE_NAME}"
echo "  ${GLUE_ROLE_NAME}"
echo "  ${EVENT_ROLE_NAME}"
echo "  ${GHA_ROLE_NAME}"

echo ""

echo "SNS Topic:"
echo "  ${SNS_TOPIC_NAME}"

echo ""

echo "EventBridge Rule:"
echo "  ${EVENT_RULE_NAME}"

echo ""

echo "All objects inside these buckets will also be deleted."
echo ""
echo "This action cannot be undone."
echo ""

read -r -p "Are you sure? (yes/no): " CONFIRM

if [[ "${CONFIRM}" != "yes" ]]; then
    echo ""
    echo "Teardown cancelled."
    exit 0
fi

# ============================================================
# STEP 1 - DELETE EVENTBRIDGE RULE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 1 - DELETING EVENTBRIDGE RULE"
echo "============================================================"
echo ""

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --name "${EVENT_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    echo "Removing EventBridge targets..."

    TARGET_IDS="$(
        aws events list-targets-by-rule \
            --profile "${AWS_PROFILE}" \
            --rule "${EVENT_RULE_NAME}" \
            --region "${REGION}" \
            --query 'Targets[].Id' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${TARGET_IDS}" && "${TARGET_IDS}" != "None" ]]; then

        aws events remove-targets \
            --profile "${AWS_PROFILE}" \
            --rule "${EVENT_RULE_NAME}" \
            --ids ${TARGET_IDS} \
            --region "${REGION}" \
            --no-cli-pager

        echo "EventBridge targets removed."

    else

        echo "No EventBridge targets found."

    fi

    echo "Deleting EventBridge rule..."

    aws events delete-rule \
        --profile "${AWS_PROFILE}" \
        --name "${EVENT_RULE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${EVENT_RULE_NAME}"

else

    echo "Not found: ${EVENT_RULE_NAME}"

fi

# ============================================================
# STEP 2 - DELETE SNS TOPIC
# ============================================================

echo ""
echo "============================================================"
echo "STEP 2 - DELETING SNS TOPIC"
echo "============================================================"
echo ""

SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${SNS_TOPIC_NAME}"

if aws sns get-topic-attributes \
    --profile "${AWS_PROFILE}" \
    --topic-arn "${SNS_TOPIC_ARN}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws sns delete-topic \
        --profile "${AWS_PROFILE}" \
        --topic-arn "${SNS_TOPIC_ARN}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${SNS_TOPIC_NAME}"

else

    echo "Not found: ${SNS_TOPIC_NAME}"

fi

# ============================================================
# STEP 3 - DELETE LAMBDA FUNCTION
# ============================================================

echo ""
echo "============================================================"
echo "STEP 3 - DELETING LAMBDA FUNCTION"
echo "============================================================"
echo ""

if aws lambda get-function \
    --profile "${AWS_PROFILE}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws lambda delete-function \
        --profile "${AWS_PROFILE}" \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${LAMBDA_FUNCTION_NAME}"

else

    echo "Not found: ${LAMBDA_FUNCTION_NAME}"

fi

# ============================================================
# STEP 4 - DELETE GLUE TABLES
# ============================================================

echo ""
echo "============================================================"
echo "STEP 4 - DELETING GLUE TABLES"
echo "============================================================"
echo ""

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    REMAINING_TABLES="$(
        aws glue get-tables \
            --profile "${AWS_PROFILE}" \
            --database-name "${GLUE_DATABASE}" \
            --region "${REGION}" \
            --query 'TableList[].Name' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${REMAINING_TABLES}" && "${REMAINING_TABLES}" != "None" ]]; then

        echo "Tables found:"
        echo "${REMAINING_TABLES}"

        for TABLE_NAME in ${REMAINING_TABLES}; do

            echo "Deleting Glue table: ${TABLE_NAME}"

            aws glue delete-table \
                --profile "${AWS_PROFILE}" \
                --database-name "${GLUE_DATABASE}" \
                --name "${TABLE_NAME}" \
                --region "${REGION}" \
                --no-cli-pager

        done

    else

        echo "No Glue tables found."

    fi

else

    echo "Glue database does not exist."

fi

# ============================================================
# STEP 5 - DELETE GLUE DATABASE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 5 - DELETING GLUE DATABASE"
echo "============================================================"
echo ""

if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws glue delete-database \
        --profile "${AWS_PROFILE}" \
        --name "${GLUE_DATABASE}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${GLUE_DATABASE}"

else

    echo "Not found: ${GLUE_DATABASE}"

fi

# ============================================================
# STEP 6 - DELETE LAMBDA IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 6 - DELETING LAMBDA IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    echo "Detaching Lambda policies..."

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${LAMBDA_ROLE_NAME}"

else

    echo "Not found: ${LAMBDA_ROLE_NAME}"

fi

# ============================================================
# STEP 7 - DELETE GLUE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 7 - DELETING GLUE IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${GLUE_ROLE_NAME}"

else

    echo "Not found: ${GLUE_ROLE_NAME}"

fi

# ============================================================
# STEP 8 - DELETE EVENTBRIDGE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 8 - DELETING EVENTBRIDGE IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${EVENT_ROLE_NAME}"

else

    echo "Not found: ${EVENT_ROLE_NAME}"

fi

# ============================================================
# STEP 9 - DELETE GITHUB ACTIONS ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 9 - DELETING GITHUB ACTIONS ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    echo "Deleting inline GitHub Actions policies..."

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GHA_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then

        for POLICY_NAME in ${INLINE_POLICIES}; do

            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GHA_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager

            echo "Deleted inline policy: ${POLICY_NAME}"

        done

    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${GHA_ROLE_NAME}"

else

    echo "Not found: ${GHA_ROLE_NAME}"

fi

# ============================================================
# STEP 10 - DELETE S3 BUCKETS
# ============================================================

echo ""
echo "============================================================"
echo "STEP 10 - DELETING S3 BUCKETS"
echo "============================================================"
echo ""

for BUCKET in "${BUCKETS[@]}"; do

    echo "Checking: ${BUCKET}"

    if aws s3api head-bucket \
        --profile "${AWS_PROFILE}" \
        --bucket "${BUCKET}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        echo "Deleting contents: ${BUCKET}"

        aws s3 rm \
            --profile "${AWS_PROFILE}" \
            "s3://${BUCKET}" \
            --recursive \
            --region "${REGION}" \
            --no-cli-pager

        echo "Deleting bucket: ${BUCKET}"

        aws s3api delete-bucket \
            --profile "${AWS_PROFILE}" \
            --bucket "${BUCKET}" \
            --region "${REGION}" \
            --no-cli-pager

        echo "Deleted: ${BUCKET}"

    else

        echo "Not found: ${BUCKET}"

    fi

    echo ""

done

# ============================================================
# SUCCESS
# ============================================================

echo "============================================================"
echo "HDB PIPELINE TEARDOWN COMPLETED"
echo "============================================================"
echo ""

echo "Deleted/checked resources:"
echo "  S3 Buckets"
echo "  Glue Tables"
echo "  Glue Database"
echo "  Lambda Function"
echo "  Lambda IAM Role"
echo "  Glue IAM Role"
echo "  EventBridge IAM Role"
echo "  GitHub Actions IAM Role"
echo "  SNS Topic"
echo "  EventBridge Rule"

echo ""
echo "AWS Account : ${ACCOUNT_ID}"
echo "AWS Region  : ${REGION}"
echo ""

echo "============================================================"