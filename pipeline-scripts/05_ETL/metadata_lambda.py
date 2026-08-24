"""
metadata_lambda.py
===================
Thin wrappers around the metadata-reader/updater Lambda (setup.sh Step 10,
mission-hdb-eventdriven-metadata-reader), reused for both reading and
writing the 4 metadata tables (metadata_tables, table_parameters,
table_watermarks, pipeline_runs). One Lambda, two actions ("read" /
"update"), one boto3 invoke call underneath - see
pipeline-scripts/01_template_creation/lambda-script/lambda_function.py for
the full payload shape it expects/returns.

NOTE: metadata_tracking.py (same folder) tracks the SAME 4 tables but talks
to Athena directly via boto3 - no Lambda involved. That's the older pattern
most job_*.py scripts assume (run_with_tracking()). This module is the
Lambda-based path pipeline_orchestration.ipynb's "Metadata snapshot"
section uses instead. Both end up reading/writing the same Iceberg tables,
just via two different routes - worth knowing if asked why there are two.

Every read/update also gets written to disk as a JSON string (context.json
by default) via _save_context() - not just kept in memory - so the
metadata state survives a kernel restart and can be eyeballed outside the
notebook (matches the context.json/lambda_test.json files test_step10.sh /
test_lambda_context_update.sh already produce in this same project root).

Usage (from pipeline_orchestration.ipynb or run_pipeline.py - both already
add pipeline-scripts/05_ETL to sys.path, so this imports the same way
config.py/common.py do):

    from metadata_lambda import create_context, update_context

    context = create_context()
    ...
    context = update_context(
        watermark_updates=[{"table_id": 1, "last_watermark_value": "...", "last_run_id": 101}],
        pipeline_run={"run_id": 101, "table_id": 1, "layer": "bronze",
                      "start_time": "...", "end_time": "...", "status": "SUCCEEDED",
                      "number_of_records": 3, "error_message": None},
    )
"""

import json
from pathlib import Path

import boto3

from config import AWS_REGION

LAMBDA_FUNCTION_NAME = "mission-hdb-eventdriven-metadata-reader"  # created by setup.sh Step 10

# Where create_context()/update_context() write the JSON snapshot to disk
# by default - the project root (matches where the bash test scripts already
# drop context.json), not this ETL/ folder. Override via the `path` kwarg on
# either function if you want it somewhere else.
DEFAULT_CONTEXT_PATH = Path(__file__).resolve().parent.parent / "04_json_scripts" / "context.json"

_lambda_client = boto3.client("lambda", region_name=AWS_REGION)


def call_metadata_lambda(action: str = "read", **payload) -> dict:
    """
    Invokes the metadata-reader Lambda synchronously. The SAME function
    handles both actions - "read" just returns current state; "update"
    applies watermark_updates/pipeline_run first, then returns the (now
    fresh) state - so the caller always gets JSON back either way.
    create_context() / update_context() below are thin, purpose-specific
    wrappers around this one call.
    """
    response = _lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"action": action, **payload}).encode("utf-8"),
    )
    result = json.loads(response["Payload"].read())
    if response.get("FunctionError"):
        raise RuntimeError(f"metadata Lambda failed: {result}")
    return result


def _save_context(ctx: dict, path: Path = DEFAULT_CONTEXT_PATH) -> str:
    """Serializes ctx to a JSON string and writes it to `path`. Returns the
    JSON string itself too, in case the caller wants it directly rather
    than re-reading the file."""
    context_json = json.dumps(ctx, indent=2, default=str)
    Path(path).write_text(context_json)
    return context_json


def load_context(path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    """Reads the JSON snapshot back from disk without calling the Lambda -
    useful to pick up where a previous run left off without paying for
    another read."""
    return json.loads(Path(path).read_text())


def create_context(path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    """READ - reads the 4 metadata tables via the Lambda, saves the result
    to `path` as JSON, and hands the snapshot back as "context" for the
    caller to act on."""
    ctx = call_metadata_lambda("read")
    if ctx.get("errors"):
        print("WARNING - some metadata tables failed to read:", ctx["errors"])
    for table_name, rows in ctx["tables"].items():
        print(f"  {table_name:20s} {len(rows)} row(s)")
    _save_context(ctx, path)
    print(f"Context saved to {path}")
    return ctx


def update_context(watermark_updates: list = None, pipeline_run: dict = None, path: Path = DEFAULT_CONTEXT_PATH) -> dict:
    """WRITE - applies a watermark bump / pipeline_run row via the SAME
    Lambda (action="update"), saves the fresh state to `path` as JSON, and
    returns it."""
    updated = call_metadata_lambda(
        "update",
        watermark_updates=watermark_updates or [],
        pipeline_run=pipeline_run,
    )
    if updated.get("update_errors"):
        raise RuntimeError(f"metadata update failed: {updated['update_errors']}")
    _save_context(updated, path)
    print(f"Context saved to {path}")
    return updated
