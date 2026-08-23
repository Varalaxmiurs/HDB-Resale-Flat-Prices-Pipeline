#!/bin/bash

set -Eeuo pipefail

# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE - SIMPLE TEARDOWN
# ============================================================

PROJECT_NAME="hdb-eventdriven"

REGION="${AWS_REGION:-ap-southeast-1}"

# ============================================================
# AWS ACCOUNT
# ============================================================

ACCOUNT_ID="$(aws sts get-caller-identity \
    --query Account \
    --output text \
    --region "${REGION}" \
    --no-cli-pager)"

# ============================================================
# BUCKETS
# ============================================================

BUCKETS=(
    "${PROJECT_NAME}-source"
    "${PROJECT_NAME}-raw"
    "${PROJECT_NAME}-cleaned"
    "${PROJECT_NAME}-transformed"
    "${PROJECT_NAME}-hashed"
    "${PROJECT_NAME}-failed"
    "${PROJECT_NAME}-pipeline-scripts"
    "${PROJECT_NAME}-audit-tables"
)

# ============================================================
# RESOURCE NAMES
# ============================================================

GLUE_DATABASE="${PROJECT_NAME}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"

EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

# ============================================================
# PERMISSION
# ============================================================

echo ""
echo "============================================================"
echo "HDB PIPELINE TEARDOWN"
echo "============================================================"
echo ""

echo "AWS Account : ${ACCOUNT_ID}"
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

echo "IAM Roles:"
echo "  ${LAMBDA_ROLE_NAME}"
echo "  ${GLUE_ROLE_NAME}"
echo "  ${EVENT_ROLE_NAME}"

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
    --name "${EVENT_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    echo "Removing EventBridge targets..."

    TARGET_IDS="$(aws events list-targets-by-rule \
        --rule "${EVENT_RULE_NAME}" \
        --region "${REGION}" \
        --query 'Targets[].Id' \
        --output text \
        --no-cli-pager 2>/dev/null || true)"

    if [[ -n "${TARGET_IDS}" && "${TARGET_IDS}" != "None" ]]; then

        aws events remove-targets \
            --rule "${EVENT_RULE_NAME}" \
            --ids ${TARGET_IDS} \
            --region "${REGION}" \
            --no-cli-pager

    fi

    echo "Deleting EventBridge rule..."

    aws events delete-rule \
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
    --topic-arn "${SNS_TOPIC_ARN}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws sns delete-topic \
        --topic-arn "${SNS_TOPIC_ARN}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${SNS_TOPIC_NAME}"

else

    echo "Not found: ${SNS_TOPIC_NAME}"

fi

# ============================================================
# STEP 3 - DELETE GLUE DATABASE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 3 - DELETING GLUE DATABASE"
echo "============================================================"
echo ""

if aws glue get-database \
    --name "${GLUE_DATABASE}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws glue delete-database \
        --name "${GLUE_DATABASE}" \
        --region "${REGION}" \
        --no-cli-pager

    echo "Deleted: ${GLUE_DATABASE}"

else

    echo "Not found: ${GLUE_DATABASE}"

fi

# ============================================================
# STEP 4 - DELETE LAMBDA IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 4 - DELETING LAMBDA IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws iam detach-role-policy \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn \
        arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --policy-arn \
        arn:aws:iam::aws:policy/AmazonS3FullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam delete-role \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${LAMBDA_ROLE_NAME}"

else

    echo "Not found: ${LAMBDA_ROLE_NAME}"

fi

# ============================================================
# STEP 5 - DELETE GLUE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 5 - DELETING GLUE IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --role-name "${GLUE_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws iam detach-role-policy \
        --role-name "${GLUE_ROLE_NAME}" \
        --policy-arn \
        arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
        --no-cli-pager 2>/dev/null || true

    aws iam detach-role-policy \
        --role-name "${GLUE_ROLE_NAME}" \
        --policy-arn \
        arn:aws:iam::aws:policy/AmazonS3FullAccess \
        --no-cli-pager 2>/dev/null || true

    aws iam delete-role \
        --role-name "${GLUE_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${GLUE_ROLE_NAME}"

else

    echo "Not found: ${GLUE_ROLE_NAME}"

fi

# ============================================================
# STEP 6 - DELETE EVENTBRIDGE IAM ROLE
# ============================================================

echo ""
echo "============================================================"
echo "STEP 6 - DELETING EVENTBRIDGE IAM ROLE"
echo "============================================================"
echo ""

if aws iam get-role \
    --role-name "${EVENT_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws iam delete-role \
        --role-name "${EVENT_ROLE_NAME}" \
        --no-cli-pager

    echo "Deleted: ${EVENT_ROLE_NAME}"

else

    echo "Not found: ${EVENT_ROLE_NAME}"

fi

# ============================================================
# STEP 7 - DELETE METADATA TABLES
# ============================================================

echo ""
echo "============================================================"
echo "STEP 7 - DELETING METADATA TABLES"
echo "============================================================"
echo ""

METADATA_TABLES=(
    "table_metadata"
    "watermarks"
    "pipeline_runs"
)

for TABLE_NAME in "${METADATA_TABLES[@]}"; do

    echo "Checking metadata table: ${TABLE_NAME}"

    if aws glue get-table \
        --database-name "${GLUE_DATABASE}" \
        --name "${TABLE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        echo "Deleting metadata table: ${TABLE_NAME}"

        aws glue delete-table \
            --database-name "${GLUE_DATABASE}" \
            --name "${TABLE_NAME}" \
            --region "${REGION}" \
            --no-cli-pager

        echo "Deleted: ${TABLE_NAME}"

    else

        echo "Not found: ${TABLE_NAME}"

    fi

done

# ============================================================
# STEP 8 - DELETE S3 BUCKETS
# ============================================================

echo ""
echo "============================================================"
echo "STEP 8- DELETING S3 BUCKETS"
echo "============================================================"
echo ""

for BUCKET in "${BUCKETS[@]}"; do

    echo "Checking: ${BUCKET}"

    if aws s3api head-bucket \
        --bucket "${BUCKET}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        echo "Deleting contents: ${BUCKET}"

        aws s3 rm \
            "s3://${BUCKET}" \
            --recursive \
            --region "${REGION}" \
            --no-cli-pager

        echo "Deleting bucket: ${BUCKET}"

        aws s3api delete-bucket \
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
echo "Deleted resources:"
echo "  S3 Buckets"
echo "  Glue Database"
echo "  Lambda IAM Role"
echo "  Glue IAM Role"
echo "  EventBridge IAM Role"
echo "  SNS Topic"
echo "  EventBridge Rule"

echo ""
echo "============================================================"