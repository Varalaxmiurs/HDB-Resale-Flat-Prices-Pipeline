#!/bin/bash
# verify_resources.sh
# ====================
# READ-ONLY check: what "hdb-eventdriven-*" resources actually exist for
# the CURRENT AWS account (whatever `aws sts get-caller-identity` resolves
# to via your active profile/credentials), across all 3 regions this
# project has touched so far (ap-south-1, ap-southeast-1, us-east-1).
# Doesn't delete or create anything - safe to run any time you're unsure
# what state things are actually in, including right after tear_down.sh
# to confirm it actually cleaned everything up.
#
# Usage: bash verify_resources.sh

set -uo pipefail  # deliberately no -e: a single "not found" shouldn't
                   # abort the whole scan, we want to see everything

PROJECT_NAME="hdb-eventdriven"
REGIONS=("ap-south-1" "ap-southeast-1" "us-east-1")

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

echo "============================================================"
echo "VERIFYING RESOURCES"
echo "============================================================"
echo "AWS Account: ${ACCOUNT_ID}"
echo ""

# --------------------------------------------------------------
# S3 buckets - list-buckets is account-wide (S3 has no per-region
# "list" API), so this only needs to run once. Each matching
# bucket's actual region is then looked up separately.
# --------------------------------------------------------------
echo "S3 buckets matching '*${PROJECT_NAME}*' (any region, old and new mission- prefix both included):"
echo ""

BUCKETS="$(aws s3api list-buckets \
    --query "Buckets[?contains(Name, \`${PROJECT_NAME}\`)].Name" \
    --output text)"

if [[ -z "${BUCKETS}" ]]; then
    echo "  (none found)"
else
    for BUCKET in ${BUCKETS}; do
        LOCATION="$(aws s3api get-bucket-location --bucket "${BUCKET}" --query LocationConstraint --output text 2>/dev/null)"
        # An empty/"None" LocationConstraint means us-east-1 - that's an
        # AWS API quirk, not a missing value.
        [[ "${LOCATION}" == "None" || -z "${LOCATION}" ]] && LOCATION="us-east-1"
        OBJECT_COUNT="$(aws s3 ls "s3://${BUCKET}" --recursive --summarize 2>/dev/null | grep "Total Objects:" | awk '{print $3}')"
        echo "  ${BUCKET}  (region: ${LOCATION}, objects: ${OBJECT_COUNT:-0})"
    done
fi

echo ""

# --------------------------------------------------------------
# Everything else IS region-scoped, so loop over all 3.
# --------------------------------------------------------------
GLUE_DATABASE="${PROJECT_NAME//-/_}_database"
LAMBDA_FUNCTION_NAME="${PROJECT_NAME}-metadata-reader"
SNS_TOPIC_NAME="${PROJECT_NAME}-notifications"
EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

for REGION in "${REGIONS[@]}"; do

    echo "============================================================"
    echo "REGION: ${REGION}"
    echo "============================================================"

    echo "Glue database (${GLUE_DATABASE}):"
    if aws glue get-database --name "${GLUE_DATABASE}" --region "${REGION}" >/dev/null 2>&1; then
        echo "  EXISTS"
        TABLES="$(aws glue get-tables --database-name "${GLUE_DATABASE}" --region "${REGION}" \
            --query "TableList[].Name" --output text 2>/dev/null)"
        if [[ -n "${TABLES}" ]]; then
            for T in ${TABLES}; do
                echo "    - ${T}"
            done
        else
            echo "    (no tables)"
        fi
    else
        echo "  not found"
    fi
    echo ""

    echo "Lambda function (${LAMBDA_FUNCTION_NAME}):"
    if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
        echo "  EXISTS"
    else
        echo "  not found"
    fi
    echo ""

    echo "SNS topic (${SNS_TOPIC_NAME}):"
    TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${SNS_TOPIC_NAME}"
    if aws sns get-topic-attributes --topic-arn "${TOPIC_ARN}" --region "${REGION}" >/dev/null 2>&1; then
        echo "  EXISTS (${TOPIC_ARN})"
    else
        echo "  not found"
    fi
    echo ""

    echo "EventBridge rule (${EVENT_RULE_NAME}):"
    if aws events describe-rule --name "${EVENT_RULE_NAME}" --region "${REGION}" >/dev/null 2>&1; then
        echo "  EXISTS"
    else
        echo "  not found"
    fi

    echo ""

done

echo "============================================================"
echo "DONE"
echo "============================================================"
echo "(IAM roles - hdb-eventdriven-lambda-role, -glue-role,"
echo "-eventbridge-role - are account-wide, not per-region. Check"
echo "those with: aws iam get-role --role-name <name>)"
