#!/bin/bash

set -Eeuo pipefail

# ============================================================
# STEP 10 - LAMBDA PACKAGE TEST
# ============================================================

echo ""
echo "============================================================"
echo "STEP 10 - LAMBDA PACKAGE TEST"
echo "============================================================"

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LAMBDA_SOURCE="${SCRIPT_DIR}/pipeline-scripts/01_template_creation/lambda-script"

LAMBDA_ZIP="${SCRIPT_DIR}/lambda_function.zip"

PROJECT_NAME="hdb-eventdriven"

REGION="us-east-1"

LAMBDA_FUNCTION_NAME="mission-${PROJECT_NAME}-metadata-reader"

echo "Project Directory : ${SCRIPT_DIR}"
echo "Lambda Source     : ${LAMBDA_SOURCE}"
echo "Lambda ZIP        : ${LAMBDA_ZIP}"
echo "Lambda Function   : ${LAMBDA_FUNCTION_NAME}"
echo "AWS Region        : ${REGION}"


# ------------------------------------------------------------
# FORCE AWS PROFILE
# ------------------------------------------------------------

export AWS_PROFILE=sujen
export AWS_REGION=us-east-1

unset AWS_DEFAULT_PROFILE


# ------------------------------------------------------------
# VERIFY AWS ACCOUNT
# ------------------------------------------------------------

echo ""
echo "Checking AWS identity..."

AWS_ACCOUNT="$(aws sts get-caller-identity \
    --query Account \
    --output text \
    --region "${REGION}")"

echo "AWS Account       : ${AWS_ACCOUNT}"

if [[ "${AWS_ACCOUNT}" != "544795558120" ]]; then

    echo ""
    echo "ERROR: Wrong AWS account."
    echo "Expected : 544795558120"
    echo "Actual   : ${AWS_ACCOUNT}"

    exit 1

fi


# ------------------------------------------------------------
# CHECK LAMBDA SOURCE
# ------------------------------------------------------------

echo ""
echo "Checking Lambda source..."

if [[ ! -d "${LAMBDA_SOURCE}" ]]; then

    echo "ERROR: Lambda source directory not found:"
    echo "${LAMBDA_SOURCE}"

    exit 1

fi

echo "Lambda source found."


# ------------------------------------------------------------
# SHOW SOURCE FILES
# ------------------------------------------------------------

echo ""
echo "Lambda source files:"

find "${LAMBDA_SOURCE}" -type f \
    ! -path "*/__pycache__/*" \
    ! -name "*.pyc" \
    ! -name ".DS_Store"


# ------------------------------------------------------------
# REMOVE OLD ZIP
# ------------------------------------------------------------

echo ""
echo "Removing old Lambda ZIP..."

rm -f "${LAMBDA_ZIP}"


# ------------------------------------------------------------
# CREATE ZIP
# ------------------------------------------------------------

echo ""
echo "Creating Lambda package..."

PYTHON_BIN="$(command -v python || command -v python3 || true)"

if [[ -z "${PYTHON_BIN}" ]]; then

    echo "ERROR: Python is not installed."

    exit 1

fi

echo "Python:"
echo "${PYTHON_BIN}"


"${PYTHON_BIN}" - "${LAMBDA_SOURCE}" "${LAMBDA_ZIP}" <<'PYEOF'

import sys
import zipfile
from pathlib import Path

source_dir = Path(sys.argv[1])
zip_path = Path(sys.argv[2])

exclude_dirs = {
    "__pycache__"
}

exclude_suffixes = {
    ".pyc"
}

exclude_names = {
    ".DS_Store"
}

with zipfile.ZipFile(
    zip_path,
    "w",
    zipfile.ZIP_DEFLATED
) as zf:

    for path in sorted(source_dir.rglob("*")):

        if not path.is_file():
            continue

        relative_path = path.relative_to(source_dir)

        if any(part in exclude_dirs for part in relative_path.parts):
            continue

        if path.suffix in exclude_suffixes:
            continue

        if path.name in exclude_names:
            continue

        zf.write(
            path,
            relative_path
        )

print(f"Created: {zip_path}")

PYEOF


# ------------------------------------------------------------
# VERIFY ZIP
# ------------------------------------------------------------

echo ""
echo "Checking Lambda ZIP..."

if [[ ! -s "${LAMBDA_ZIP}" ]]; then

    echo "ERROR: Lambda ZIP was not created."

    exit 1

fi

echo "Lambda ZIP created successfully."

ls -lh "${LAMBDA_ZIP}"


# ------------------------------------------------------------
# CONVERT GIT BASH PATH TO WINDOWS PATH
# ------------------------------------------------------------

echo ""
echo "Converting Lambda ZIP path..."

if command -v cygpath >/dev/null 2>&1; then

    LAMBDA_ZIP_WIN="$(cygpath -w "${LAMBDA_ZIP}")"

else

    echo "ERROR: cygpath is not available."

    exit 1

fi

echo "Git Bash path:"
echo "${LAMBDA_ZIP}"

echo ""
echo "Windows path:"
echo "${LAMBDA_ZIP_WIN}"


# ------------------------------------------------------------
# VERIFY ZIP CONTENT
# ------------------------------------------------------------

echo ""
echo "Lambda ZIP contents:"

"${PYTHON_BIN}" - "${LAMBDA_ZIP}" <<'PYEOF'

import sys
import zipfile

zip_path = sys.argv[1]

with zipfile.ZipFile(zip_path, "r") as z:

    for name in z.namelist():
        print(name)

PYEOF


# ------------------------------------------------------------
# VERIFY LAMBDA IAM ROLE
# ------------------------------------------------------------

LAMBDA_ROLE_NAME="hdb-eventdriven-lambda-role"

echo ""
echo "Checking Lambda IAM role..."

if aws iam get-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --query "Role.Arn" \
    --output text \
    >/tmp/lambda_role_arn.txt 2>/tmp/lambda_role_error.txt
then

    LAMBDA_ROLE_ARN="$(cat /tmp/lambda_role_arn.txt)"

    echo "Lambda Role ARN:"
    echo "${LAMBDA_ROLE_ARN}"

else

    echo "WARNING: Lambda IAM role does not exist yet."

    cat /tmp/lambda_role_error.txt

    echo ""
    echo "ZIP TEST PASSED."
    echo "Lambda creation was NOT attempted."

    rm -f /tmp/lambda_role_arn.txt
    rm -f /tmp/lambda_role_error.txt

    exit 0

fi


rm -f /tmp/lambda_role_arn.txt
rm -f /tmp/lambda_role_error.txt


# ------------------------------------------------------------
# CHECK EXISTING LAMBDA
# ------------------------------------------------------------

echo ""
echo "Checking Lambda function..."

if aws lambda get-function \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then

    echo "Lambda function exists:"
    echo "${LAMBDA_FUNCTION_NAME}"

    echo ""
    echo "Testing AWS CLI ZIP path..."

    aws lambda update-function-code \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP_WIN}" \
        --region "${REGION}" \
        --publish

    echo ""
    echo "Lambda code update successful."

else

    echo "Lambda function does not exist."

    echo ""
    echo "ZIP creation and Windows path conversion were successful."

    echo "Lambda creation was NOT attempted."

fi


# ------------------------------------------------------------
# CLEAN TEMPORARY ZIP
# ------------------------------------------------------------

echo ""
echo "Keeping ZIP for inspection:"
echo "${LAMBDA_ZIP}"

echo ""
echo "============================================================"
echo "STEP 10 TEST COMPLETED"
echo "============================================================"
