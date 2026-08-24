#!/bin/bash

set -Eeuo pipefail

# ============================================================
# STEP 10 - LAMBDA PACKAGE TEST
# ============================================================

echo ""
echo "============================================================"
echo "STEP 10 - LAMBDA PACKAGE TEST"
echo "============================================================"


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

LAMBDA_SOURCE="${PROJECT_ROOT}/pipeline-scripts/01_template_creation/lambda-script"

LAMBDA_ZIP="${PROJECT_ROOT}/pipeline-scripts/lambda_function.zip"

PROJECT_NAME="hdb-eventdriven"

REGION="us-east-1"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"
LAMBDA_ROLE_NAME="hdb-eventdriven-lambda-role"

# same values config.py / setup.sh use - the Lambda's env vars must match
GLUE_DATABASE="hdb_eventdriven_database"
ATHENA_WORKGROUP="primary"
AUDIT_BUCKET="mission-${PROJECT_NAME}-audit-tables"


echo "Project Root      : ${PROJECT_ROOT}"
echo "Lambda Source     : ${LAMBDA_SOURCE}"
echo "Lambda ZIP        : ${LAMBDA_ZIP}"
echo "Lambda Function   : ${LAMBDA_FUNCTION_NAME}"
echo "AWS Region        : ${REGION}"


# ============================================================
# AWS PROFILE
# ============================================================

export AWS_PROFILE=sujen
export AWS_REGION=us-east-1

unset AWS_DEFAULT_PROFILE


# ============================================================
# VERIFY AWS ACCOUNT
# ============================================================

echo ""
echo "Checking AWS identity..."

AWS_ACCOUNT="$(aws sts get-caller-identity \
    --profile sujen \
    --query Account \
    --output text \
    --region us-east-1)"

echo "AWS Account       : ${AWS_ACCOUNT}"

if [[ "${AWS_ACCOUNT}" != "544795558120" ]]; then
    echo "ERROR: Wrong AWS account."
    exit 1
fi


# ============================================================
# CHECK LAMBDA SOURCE
# ============================================================

echo ""
echo "Checking Lambda source..."

if [[ ! -d "${LAMBDA_SOURCE}" ]]; then

    echo "ERROR: Lambda source directory not found:"
    echo "${LAMBDA_SOURCE}"

    echo ""
    echo "Searching for lambda-script directories..."

    find "${PROJECT_ROOT}" \
        -type d \
        -name "lambda-script" \
        -print

    exit 1
fi

echo "Lambda source found."


# ============================================================
# SHOW SOURCE FILES
# ============================================================

echo ""
echo "Lambda source files:"

find "${LAMBDA_SOURCE}" \
    -type f \
    ! -path "*/__pycache__/*" \
    ! -name "*.pyc" \
    ! -name ".DS_Store"


# ============================================================
# CREATE ZIP
# ============================================================

echo ""
echo "Creating Lambda ZIP..."

rm -f "${LAMBDA_ZIP}"

PYTHON_BIN="$(command -v python || command -v python3 || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
    echo "ERROR: Python is not available."
    exit 1
fi


"${PYTHON_BIN}" - "${LAMBDA_SOURCE}" "${LAMBDA_ZIP}" <<'PYEOF'

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

        if path.name == "test_step10.sh":
            continue

        zf.write(path, relative_path)

print("ZIP created successfully.")

PYEOF


# ============================================================
# VERIFY ZIP
# ============================================================

echo ""
echo "Checking ZIP..."

if [[ ! -s "${LAMBDA_ZIP}" ]]; then
    echo "ERROR: ZIP file was not created."
    exit 1
fi

ls -lh "${LAMBDA_ZIP}"


# ============================================================
# WINDOWS PATH
# ============================================================

echo ""
echo "Converting Git Bash path..."

LAMBDA_ZIP_WIN="$(cygpath -w "${LAMBDA_ZIP}")"

echo "Git Bash path:"
echo "${LAMBDA_ZIP}"

echo ""
echo "Windows path:"
echo "${LAMBDA_ZIP_WIN}"


# ============================================================
# ZIP CONTENT
# ============================================================

echo ""
echo "ZIP contents:"

"${PYTHON_BIN}" - "${LAMBDA_ZIP}" <<'PYEOF'

import sys
import zipfile

with zipfile.ZipFile(sys.argv[1], "r") as z:
    for name in z.namelist():
        print(name)

PYEOF


# ============================================================
# CHECK LAMBDA ROLE
# ============================================================

echo ""
echo "Checking Lambda IAM role..."

LAMBDA_ROLE_ARN="$(aws iam get-role \
    --profile sujen \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --query "Role.Arn" \
    --output text \
    --region us-east-1 2>/dev/null || true)"

if [[ -z "${LAMBDA_ROLE_ARN}" || "${LAMBDA_ROLE_ARN}" == "None" ]]; then

    echo "Lambda IAM role does not exist yet."

    echo ""
    echo "============================================================"
    echo "ZIP TEST SUCCESSFUL"
    echo "============================================================"

    echo ""
    echo "The Lambda package is ready:"
    echo "${LAMBDA_ZIP}"

    exit 0
fi


echo "Lambda Role ARN:"
echo "${LAMBDA_ROLE_ARN}"


# ============================================================
# TEST EXISTING LAMBDA
# ============================================================

echo ""
echo "Checking Lambda function..."

if aws lambda get-function \
    --profile sujen \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region us-east-1 \
    >/dev/null 2>&1
then

    echo "Lambda exists."
    echo "Testing ZIP upload..."

    aws lambda update-function-code \
        --profile sujen \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP_WIN}" \
        --region us-east-1 \
        >/dev/null

    echo ""
    echo "Lambda ZIP upload SUCCESSFUL."

else

    echo "Lambda function does not exist yet."
    echo "Creating it now..."

    aws lambda create-function \
        --profile sujen \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.12 \
        --role "${LAMBDA_ROLE_ARN}" \
        --handler lambda_function.lambda_handler \
        --zip-file "fileb://${LAMBDA_ZIP_WIN}" \
        --timeout 60 \
        --memory-size 256 \
        --environment "Variables={GLUE_DATABASE=${GLUE_DATABASE},ATHENA_WORKGROUP=${ATHENA_WORKGROUP},AUDIT_BUCKET=${AUDIT_BUCKET},AWS_REGION_NAME=${REGION}}" \
        --region us-east-1 \
        >/dev/null

    echo ""
    echo "Lambda function CREATED:"
    echo "${LAMBDA_FUNCTION_NAME}"

fi


echo ""
echo "============================================================"
echo "STEP 10 TEST COMPLETED"
echo "============================================================"

echo ""
echo "ZIP kept for inspection:"
echo "${LAMBDA_ZIP}"

