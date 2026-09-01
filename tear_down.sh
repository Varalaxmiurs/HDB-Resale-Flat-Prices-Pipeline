#!/bin/bash

set -Eeuo pipefail

# ============================================================
# HDB EVENT-DRIVEN ICEBERG PIPELINE - TEARDOWN
# ============================================================

PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"

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
# SETUP USES (setup.sh, "BUCKET NAMES" section, defined after ACCOUNT_ID
# is resolved - see that file's comment for why the account ID is in the
# name at all: S3 bucket names are a GLOBAL namespace, and a bare
# "mission-${PROJECT_NAME}-*" name collided with some OTHER AWS account's
# existing bucket for real):
#
# PROJECT_NAME="${PROJECT_NAME:-hdb-eventdriven}"
# BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"
#
# Therefore (for account 544795558120):
#
# mission-hdb-eventdriven-544795558120-source
# mission-hdb-eventdriven-544795558120-raw
# mission-hdb-eventdriven-544795558120-cleaned
# etc.
#
# Must stay in sync with setup.sh's BUCKET_PREFIX - this file computes it
# independently rather than sourcing setup.sh, so a change to one needs
# the same change here.
# ============================================================

BUCKET_PREFIX="mission-${PROJECT_NAME}-${ACCOUNT_ID}"

BUCKETS=(
    "${BUCKET_PREFIX}-source"
    "${BUCKET_PREFIX}-raw"
    "${BUCKET_PREFIX}-cleaned"
    "${BUCKET_PREFIX}-transformed"
    "${BUCKET_PREFIX}-hashed"
    "${BUCKET_PREFIX}-failed"
    "${BUCKET_PREFIX}-pipeline-scripts"
    "${BUCKET_PREFIX}-audit-tables"
    # Glue-managed, not one of setup.sh's own bucket names - AWS Glue
    # auto-creates this the first time a Glue Studio/interactive-session
    # feature is used in this account/region, named by a fixed
    # aws-glue-assets-<account_id>-<region> convention (not project-scoped,
    # so it survives independent of PROJECT_NAME/BUCKET_PREFIX). Included
    # here so a full teardown actually removes every bucket this project
    # has touched, not just the ones it explicitly provisioned.
    "aws-glue-assets-${ACCOUNT_ID}-${REGION}"
)

# ============================================================
# RESOURCE NAMES
# ============================================================

# BUG FIXED HERE: this used to be "${PROJECT_NAME}_database" (no hyphen
# substitution) while setup.sh creates the REAL database as
# "${PROJECT_NAME//-/_}_database" (Glue database names can't contain
# hyphens, so setup.sh replaces them with underscores). For
# PROJECT_NAME=hdb-eventdriven-test that meant this script was checking
# for/deleting "hdb-eventdriven-test_database" - a name that never
# existed - while the REAL database "hdb_eventdriven_test_database" (all
# underscores) sat there completely untouched. Every "Not found" this
# script ever printed for the Glue database was actually true for the
# WRONG name it was looking for, not proof the real one was gone. Fixed
# to derive the name the exact same way setup.sh does.
GLUE_DATABASE="${PROJECT_NAME//-/_}_database"

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

GLUE_ROLE_NAME="${PROJECT_NAME}-glue-role"

EVENT_ROLE_NAME="${PROJECT_NAME}-eventbridge-role"

STEPFUNCTIONS_ROLE_NAME="${PROJECT_NAME}-stepfunctions-role"

STATE_MACHINE_NAME="${PROJECT_NAME}-pipeline"

# Must match setup.sh's GLUE_JOB_1..5 exactly (hardcoded there too, to
# match orchestration.py's GLUE_JOB_NAMES dict).
GLUE_JOB_1="hdb-job-1-ingestion-to-source"
GLUE_JOB_2="hdb-job-2-raw-iceberg"
GLUE_JOB_2B="hdb-job-2b-data-profiling"
GLUE_JOB_3="hdb-job-3-cleaned-iceberg"
GLUE_JOB_4="hdb-job-4-transformed-iceberg"
GLUE_JOB_5="hdb-job-5-hashed-iceberg"

GHA_ROLE_NAME="${GITHUB_ACTIONS_ROLE_NAME:-hdb-pipeline-github-actions}"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

EVENT_RULE_NAME="${PROJECT_NAME}-pipeline-trigger"

INGESTION_TRIGGER_RULE_NAME="${PROJECT_NAME}-ingestion-complete-trigger"

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
echo "PROJECT_NAME: ${PROJECT_NAME}"
echo ""

# HDB_SKIP_CONFIRM=1 bypasses this prompt - used by setup.sh's own
# automatic rollback (see setup.sh's rollback() function), which calls
# this script unattended after a failed setup run. Never set this
# yourself when running tear_down.sh by hand - the interactive prompt is
# the safety net for a command that permanently deletes real resources.
if [[ "${HDB_SKIP_CONFIRM:-0}" == "1" ]]; then
    echo "Are you sure? (yes/no): yes  (auto-confirmed - HDB_SKIP_CONFIRM=1)"
else
    read -r -p "Are you sure? (yes/no): " CONFIRM

    if [[ "${CONFIRM}" != "yes" ]]; then
        echo ""
        echo "Teardown cancelled."
        exit 0
    fi
fi

# Everything below runs silently (no per-step banner, no per-resource
# "Deleted"/"Not found" line) - only a real problem prints anything
# between here and the final summary. That's a deliberate trade: this
# script's job is to actually delete things correctly, not narrate every
# API call - AWS Glue/S3/IAM latency is the real time cost either way,
# never the echo statements.
echo "Deleting resources..."

# ============================================================
# STEP 0 - DELETE STEP FUNCTIONS STATE MACHINE
# ============================================================

STATE_MACHINE_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"

if aws stepfunctions describe-state-machine \
    --profile "${AWS_PROFILE}" \
    --region "${REGION}" \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --no-cli-pager >/dev/null 2>&1
then

    aws stepfunctions delete-state-machine \
        --profile "${AWS_PROFILE}" \
        --region "${REGION}" \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 1 - DELETE EVENTBRIDGE RULE
# ============================================================

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --name "${EVENT_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

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
            --no-cli-pager \
            >/dev/null
    fi

    aws events delete-rule \
        --profile "${AWS_PROFILE}" \
        --name "${EVENT_RULE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

fi

if aws events describe-rule \
    --profile "${AWS_PROFILE}" \
    --name "${INGESTION_TRIGGER_RULE_NAME}" \
    --region "${REGION}" \
    --no-cli-pager >/dev/null 2>&1
then

    TARGET_IDS="$(
        aws events list-targets-by-rule \
            --profile "${AWS_PROFILE}" \
            --rule "${INGESTION_TRIGGER_RULE_NAME}" \
            --region "${REGION}" \
            --query 'Targets[].Id' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${TARGET_IDS}" && "${TARGET_IDS}" != "None" ]]; then
        aws events remove-targets \
            --profile "${AWS_PROFILE}" \
            --rule "${INGESTION_TRIGGER_RULE_NAME}" \
            --ids ${TARGET_IDS} \
            --region "${REGION}" \
            --no-cli-pager \
            >/dev/null
    fi

    aws events delete-rule \
        --profile "${AWS_PROFILE}" \
        --name "${INGESTION_TRIGGER_RULE_NAME}" \
        --region "${REGION}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 2 - DELETE SNS TOPIC
# ============================================================

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
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 3 - DELETE LAMBDA FUNCTION
# ============================================================

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
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 4 - DELETE GLUE TABLES
# ============================================================

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
        for TABLE_NAME in ${REMAINING_TABLES}; do
            aws glue delete-table \
                --profile "${AWS_PROFILE}" \
                --database-name "${GLUE_DATABASE}" \
                --name "${TABLE_NAME}" \
                --region "${REGION}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

fi

# ============================================================
# STEP 5 - DELETE GLUE DATABASE
# ============================================================

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
        --no-cli-pager \
        >/dev/null

    # PROOF, not a guess: delete-database returning exit 0 only means AWS
    # accepted the request - Glue's own docs say it can clean up the
    # database's tables ASYNCHRONOUSLY afterward. Re-check right here
    # rather than trusting the exit code, so a real failure (permissions,
    # Lake Formation, anything) is caught and reported NOW, not
    # discovered later via a stale-looking console.
    if aws glue get-database \
        --profile "${AWS_PROFILE}" \
        --name "${GLUE_DATABASE}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then
        echo ""
        echo "ERROR: Glue database ${GLUE_DATABASE} still exists after delete-database."
        echo "Check IAM/Lake Formation permissions for ${AWS_PROFILE}, or delete it"
        echo "manually: aws glue delete-database --name ${GLUE_DATABASE} --region ${REGION}"
        exit 1
    fi

fi

# ============================================================
# STEP 5B - DELETE GLUE JOBS
# ============================================================

for JOB_NAME in "${GLUE_JOB_1}" "${GLUE_JOB_2}" "${GLUE_JOB_2B}" "${GLUE_JOB_3}" "${GLUE_JOB_4}" "${GLUE_JOB_5}"; do

    if aws glue get-job \
        --profile "${AWS_PROFILE}" \
        --job-name "${JOB_NAME}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        aws glue delete-job \
            --profile "${AWS_PROFILE}" \
            --job-name "${JOB_NAME}" \
            --region "${REGION}" \
            --no-cli-pager \
            >/dev/null

    fi

done

# ============================================================
# STEP 6 - DELETE LAMBDA IAM ROLE
# ============================================================

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    # FIXED (2026-08-25): a hardcoded list of 4 policy ARNs used to be
    # detached here - if the role ever had a DIFFERENT/extra managed policy
    # attached (manually, or by a later setup.sh change), delete-role failed
    # with "DeleteConflict: Cannot delete entity, must detach all policies
    # first" - hit for real. Now detaches whatever is ACTUALLY attached,
    # discovered live via list-attached-role-policies, so this can never
    # drift out of sync with setup.sh again.
    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    # Also clear any INLINE policies - delete-role fails on those too, and
    # the old version never checked for them on this role at all.
    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${LAMBDA_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${LAMBDA_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 7 - DELETE GLUE IAM ROLE
# ============================================================

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GLUE_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    # FIXED (2026-08-25): same fix as LAMBDA_ROLE_NAME above - detach
    # whatever managed policies are ACTUALLY attached (live lookup) instead
    # of a hardcoded 2-ARN list, to avoid DeleteConflict if this role ever
    # picks up an extra policy.
    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GLUE_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GLUE_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GLUE_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GLUE_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GLUE_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 8 - DELETE EVENTBRIDGE IAM ROLE
# ============================================================

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${EVENT_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    # FIXED (2026-08-25): also detach any ATTACHED (managed) policies, not
    # just inline ones - same DeleteConflict risk as the Lambda/Glue roles
    # above if this role ever picks up a managed policy.
    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${EVENT_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${EVENT_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${EVENT_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${EVENT_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${EVENT_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 8B - DELETE STEP FUNCTIONS IAM ROLE
# ============================================================

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    # FIXED (2026-08-25): also detach any ATTACHED (managed) policies, not
    # just inline ones - same DeleteConflict risk as above.
    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

    INLINE_POLICIES="$(
        aws iam list-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
            --query 'PolicyNames[]' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${INLINE_POLICIES}" && "${INLINE_POLICIES}" != "None" ]]; then
        for POLICY_NAME in ${INLINE_POLICIES}; do
            aws iam delete-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
                --policy-name "${POLICY_NAME}" \
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${STEPFUNCTIONS_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 9 - DELETE GITHUB ACTIONS ROLE
# ============================================================

if aws iam get-role \
    --profile "${AWS_PROFILE}" \
    --role-name "${GHA_ROLE_NAME}" \
    --no-cli-pager >/dev/null 2>&1
then

    # FIXED (2026-08-25): also detach any ATTACHED (managed) policies, not
    # just inline ones - same DeleteConflict risk as above.
    ATTACHED_POLICY_ARNS="$(
        aws iam list-attached-role-policies \
            --profile "${AWS_PROFILE}" \
            --role-name "${GHA_ROLE_NAME}" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --no-cli-pager 2>/dev/null || true
    )"

    if [[ -n "${ATTACHED_POLICY_ARNS}" && "${ATTACHED_POLICY_ARNS}" != "None" ]]; then
        for POLICY_ARN in ${ATTACHED_POLICY_ARNS}; do
            aws iam detach-role-policy \
                --profile "${AWS_PROFILE}" \
                --role-name "${GHA_ROLE_NAME}" \
                --policy-arn "${POLICY_ARN}" \
                --no-cli-pager 2>/dev/null || true
        done
    fi

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
                --no-cli-pager \
                >/dev/null
        done
    fi

    aws iam delete-role \
        --profile "${AWS_PROFILE}" \
        --role-name "${GHA_ROLE_NAME}" \
        --no-cli-pager \
        >/dev/null

fi

# ============================================================
# STEP 10 - DELETE S3 BUCKETS
# ============================================================

for BUCKET in "${BUCKETS[@]}"; do

    if aws s3api head-bucket \
        --profile "${AWS_PROFILE}" \
        --bucket "${BUCKET}" \
        --region "${REGION}" \
        --no-cli-pager >/dev/null 2>&1
    then

        aws s3 rm \
            --profile "${AWS_PROFILE}" \
            "s3://${BUCKET}" \
            --recursive \
            --region "${REGION}" \
            --no-cli-pager \
            --quiet

        aws s3api delete-bucket \
            --profile "${AWS_PROFILE}" \
            --bucket "${BUCKET}" \
            --region "${REGION}" \
            --no-cli-pager

    fi

done

# ============================================================
# SUCCESS
# ============================================================

echo "============================================================"
echo "HDB PIPELINE TEARDOWN COMPLETED"
echo "============================================================"
echo ""

echo "Deleted/checked resources:"
echo "  Step Functions State Machine"
echo "  S3 Buckets"
echo "  Glue Tables"
echo "  Glue Database"
echo "  Glue Jobs"
echo "  Lambda Function"
echo "  Lambda IAM Role"
echo "  Glue IAM Role"
echo "  EventBridge IAM Role"
echo "  Step Functions IAM Role"
echo "  GitHub Actions IAM Role"
echo "  SNS Topic"
echo "  EventBridge Rule"
echo "  Ingestion-Complete Trigger Rule"

echo ""
echo "AWS Account : ${ACCOUNT_ID}"
echo "AWS Region  : ${REGION}"
echo ""

echo "============================================================"
