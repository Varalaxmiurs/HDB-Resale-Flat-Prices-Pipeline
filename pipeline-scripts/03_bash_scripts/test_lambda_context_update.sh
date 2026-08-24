#!/bin/bash

set -Eeuo pipefail

# ============================================================
# TEST: LAMBDA READ -> BUILD CONTEXT -> UPDATE FROM THAT CONTEXT
# ============================================================
# Exercises the full reuse pattern lambda_function.py is built for:
#   1. invoke with {"action": "read"}   -> current state of the 4
#      metadata tables ("context")
#   2. build an update payload FROM that context (real table_id,
#      next free run_id - not hardcoded guesses)
#   3. invoke with {"action": "update", ...} using that payload
#   4. verify the returned state actually reflects the update
#
# Requires the Lambda (mission-hdb-eventdriven-metadata-reader) to
# already exist - run ./test_step10.sh first if it doesn't.
#
# Usage: ./test_lambda_context_update.sh

echo ""
echo "============================================================"
echo "LAMBDA TEST - READ CONTEXT, THEN UPDATE FROM THAT CONTEXT"
echo "============================================================"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

LAMBDA_FUNCTION_NAME="mission-hdb-eventdriven-metadata-reader"
REGION="us-east-1"

READ_RESPONSE="${PROJECT_ROOT}/lambda_response_read.json"
UPDATE_PAYLOAD="${PROJECT_ROOT}/lambda_payload_update.json"
UPDATE_RESPONSE="${PROJECT_ROOT}/lambda_response_update.json"

export AWS_PROFILE=sujen
export AWS_REGION=us-east-1

unset AWS_DEFAULT_PROFILE


# ============================================================
# PYTHON
# ============================================================

PYTHON_BIN="$(command -v python || command -v python3 || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
    echo "ERROR: Python is not available."
    exit 1
fi


# ============================================================
# GIT BASH -> WINDOWS PATH (same fileb:// gotcha as the zip upload)
# ============================================================

if command -v cygpath >/dev/null 2>&1; then
    UPDATE_PAYLOAD_FILEB="fileb://$(cygpath -m "${UPDATE_PAYLOAD}")"
else
    UPDATE_PAYLOAD_FILEB="fileb://${UPDATE_PAYLOAD}"
fi


# ============================================================
# CHECK LAMBDA EXISTS
# ============================================================

echo ""
echo "Checking Lambda function exists..."

if ! aws lambda get-function \
    --profile sujen \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region "${REGION}" \
    >/dev/null 2>&1
then
    echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' does not exist."
    echo "Run ./test_step10.sh first to create it."
    exit 1
fi

echo "Lambda function found."


# ============================================================
# STEP 1 - READ (build context)
# ============================================================

echo ""
echo "------------------------------------------------------------"
echo "STEP 1 - READ (build context)"
echo "------------------------------------------------------------"

aws lambda invoke \
    --profile sujen \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{"action": "read"}' \
    "${READ_RESPONSE}" \
    >/dev/null

echo "Response saved to lambda_response_read.json"
echo ""
echo "Summary:"

"${PYTHON_BIN}" - "${READ_RESPONSE}" <<'PYEOF'
import json
import sys

with open(sys.argv[1]) as f:
    resp = json.load(f)

if "errorMessage" in resp:
    print("LAMBDA ERRORED:", resp["errorMessage"])
    sys.exit(1)

for table, rows in resp.get("tables", {}).items():
    print(f"  {table}: {len(rows)} row(s)")

if resp.get("errors"):
    print("  read errors:", resp["errors"])
PYEOF


# ============================================================
# STEP 2 - BUILD UPDATE PAYLOAD FROM THAT CONTEXT
# ============================================================

echo ""
echo "------------------------------------------------------------"
echo "STEP 2 - BUILD UPDATE PAYLOAD FROM CONTEXT"
echo "------------------------------------------------------------"

"${PYTHON_BIN}" - "${READ_RESPONSE}" "${UPDATE_PAYLOAD}" <<'PYEOF'
import datetime
import json
import sys

read_path, out_path = sys.argv[1], sys.argv[2]

with open(read_path) as f:
    resp = json.load(f)

watermarks = resp["tables"].get("table_watermarks", [])
runs = resp["tables"].get("pipeline_runs", [])

# Target the first real watermark row from the context - fall back to
# table_id 1 only if table_watermarks is genuinely empty (freshly-created
# metadata tables with no rows seeded yet).
table_id = int(watermarks[0]["table_id"]) if watermarks else 1

# Next free run_id = max existing run_id + 1, so re-running this script
# never collides with a row a previous run already inserted.
existing_ids = [int(r["run_id"]) for r in runs if r.get("run_id") is not None]
next_run_id = max(existing_ids, default=0) + 1

now = datetime.datetime.utcnow()
start = now.isoformat()
end = (now + datetime.timedelta(seconds=3)).isoformat()
watermark_value = now.isoformat()

payload = {
    "action": "update",
    "watermark_updates": [
        {
            "table_id": table_id,
            "last_watermark_value": watermark_value,
            "last_run_id": next_run_id,
        }
    ],
    "pipeline_run": {
        "run_id": next_run_id,
        "table_id": table_id,
        "layer": "test",
        "start_time": start,
        "end_time": end,
        "status": "SUCCEEDED",
        "number_of_records": 0,
        "error_message": None,
    },
}

with open(out_path, "w") as f:
    json.dump(payload, f)

print(f"Target table_id     : {table_id}")
print(f"New run_id          : {next_run_id}")
print(f"New watermark value : {watermark_value}")
PYEOF

echo ""
echo "Update payload written to lambda_payload_update.json"


# ============================================================
# STEP 3 - UPDATE (apply, then re-read - same call, same response shape)
# ============================================================

echo ""
echo "------------------------------------------------------------"
echo "STEP 3 - UPDATE (apply, then re-read)"
echo "------------------------------------------------------------"

aws lambda invoke \
    --profile sujen \
    --region "${REGION}" \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload "${UPDATE_PAYLOAD_FILEB}" \
    "${UPDATE_RESPONSE}" \
    >/dev/null

echo "Response saved to lambda_response_update.json"


# ============================================================
# STEP 4 - VERIFY the update actually landed
# ============================================================

echo ""
echo "------------------------------------------------------------"
echo "STEP 4 - VERIFY"
echo "------------------------------------------------------------"

if "${PYTHON_BIN}" - "${UPDATE_PAYLOAD}" "${UPDATE_RESPONSE}" <<'PYEOF'
import json
import sys

payload_path, resp_path = sys.argv[1], sys.argv[2]

with open(payload_path) as f:
    payload = json.load(f)
with open(resp_path) as f:
    resp = json.load(f)

if "errorMessage" in resp:
    print("LAMBDA ERRORED:", resp["errorMessage"])
    sys.exit(1)

if resp.get("update_errors"):
    print("UPDATE ERRORS:")
    for e in resp["update_errors"]:
        print(" ", e)
    sys.exit(1)

expected_table_id = payload["watermark_updates"][0]["table_id"]
expected_wm = payload["watermark_updates"][0]["last_watermark_value"]
expected_run_id = payload["pipeline_run"]["run_id"]

watermarks = resp["tables"].get("table_watermarks", [])
runs = resp["tables"].get("pipeline_runs", [])

wm_row = next((w for w in watermarks if str(w.get("table_id")) == str(expected_table_id)), None)
run_row = next((r for r in runs if str(r.get("run_id")) == str(expected_run_id)), None)

ok = True

if wm_row and wm_row.get("last_watermark_value") == expected_wm:
    print(f"OK    table_watermarks[table_id={expected_table_id}].last_watermark_value updated correctly")
else:
    print(f"FAIL  table_watermarks[table_id={expected_table_id}] did not reflect the new value")
    print("      got:", wm_row)
    ok = False

if run_row:
    print(f"OK    pipeline_runs now contains run_id={expected_run_id}")
else:
    print(f"FAIL  pipeline_runs has no row for run_id={expected_run_id}")
    ok = False

sys.exit(0 if ok else 1)
PYEOF
then
    echo ""
    echo "============================================================"
    echo "LAMBDA READ + UPDATE TEST PASSED"
    echo "============================================================"
else
    echo ""
    echo "============================================================"
    echo "LAMBDA READ + UPDATE TEST FAILED"
    echo "============================================================"
    exit 1
fi
