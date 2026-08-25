"""
orchestration.py
=================
All pipeline-EXECUTION logic for the HDB Resale Flat Prices pipeline:
running each stage (locally in-process, or via an already-deployed AWS
Glue job), sending SNS alerts, and looping through the full chain with
fail-fast behaviour.

Imported by BOTH pipeline_orchestration.ipynb and run_pipeline.py, so this
logic exists in exactly one place instead of being copy-pasted into both
(which is how it used to be - this file replaces that duplication).

Usage:

    from orchestration import run_pipeline

    run_pipeline()                                  # local mode, all 6 steps
    run_pipeline(run_mode="glue")                    # trigger real Glue jobs instead
    run_pipeline(skip_ingestion=True)                 # source files already exist
    run_pipeline(include_hashed_step=False)           # stop before job_5
    run_pipeline(only_step="cleaned_iceberg")          # debug: just this one step

Only "local" mode actually works today - "glue" mode needs the 6 AWS Glue
Job resources to exist first, and setup.sh currently provisions the Glue
database/IAM role/S3 script upload but never registers the Glue Jobs
themselves.

METADATA TRACKING - ONE consolidated update per pipeline run, not one per
layer. Earlier this session every step individually called the metadata
Lambda (read context -> run -> update context -> verify), so a normal
5-step run produced 5 separate pipeline_runs rows and 5 separate context
reads/writes. That's now consolidated: each step just runs (job.main()
directly, no Lambda calls per step - see run_local_step()), and AFTER the
whole loop finishes, run_pipeline() does exactly ONE metadata read + ONE
metadata write, logging a SINGLE pipeline_runs row for the entire run
(layer="pipeline", or the step's own name if only one step ran via
only_step=). context_tracking.py's run_step_with_context() (the old
per-layer wrapper) is still there and still works - it's just no longer
what the main loop calls by default.
"""

import time
from datetime import datetime

import boto3

from config import AWS_REGION
from common import get_table_parameter, send_alert
from metadata_lambda import DEFAULT_CONTEXT_PATH, create_context, update_context
from context_tracking import compute_next_run_id, resolve_target_table_id, verify_context_update

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

# Ordered pipeline steps. data_profiling (job_2b) sits between raw_iceberg
# and cleaned_iceberg: it profiles raw_iceberg and its output is the
# statistical basis for cleaned_iceberg's field-validation rules.
BASE_PIPELINE_STEPS = [
    "ingestion_to_source",
    "raw_iceberg",
    "data_profiling",
    "cleaned_iceberg",
    "transformed_iceberg",
]

# Map of pipeline step name -> actual Glue job name (as created in AWS Glue).
# Only used when run_mode == "glue".
GLUE_JOB_NAMES = {
    "ingestion_to_source": "hdb-job-1-ingestion-to-source",
    "raw_iceberg":         "hdb-job-2-raw-iceberg",
    "data_profiling":      "hdb-job-2b-data-profiling",
    "cleaned_iceberg":     "hdb-job-3-cleaned-iceberg",
    "transformed_iceberg": "hdb-job-4-transformed-iceberg",
    "hashed_iceberg":      "hdb-job-5-hashed-iceberg",
}

GLUE_POLL_INTERVAL_SECONDS = 15  # how often we poll Glue for job-run status (glue mode only)
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}


# Every valid step name, in pipeline order - used to validate only_step=...
# below (a typo there should fail loudly, not silently run nothing).
ALL_STEPS = BASE_PIPELINE_STEPS + ["hashed_iceberg"]


def build_pipeline_steps(include_hashed_step: bool = True, skip_ingestion: bool = False, only_step: str = None) -> list:
    """Ordered step list for this run.

    only_step='cleaned_iceberg' (etc.) - DEBUG ESCAPE HATCH: run just that
    ONE step and nothing else, ignoring include_hashed_step/skip_ingestion
    entirely. Useful while iterating on a single stage - e.g. you fixed a
    bug in job_3 and just want to rerun cleaned_iceberg against the
    raw_iceberg that's already sitting there, without re-running ingestion
    or raw_iceberg again first. Every step reads its input from the
    PREVIOUS stage's Iceberg table (job_3 reads raw_iceberg, job_4 reads
    cleaned_iceberg, ...), so the step before whichever one you pick must
    already have real data in it for this to do anything useful.

    Otherwise: hashed_iceberg (job_5) is appended unless
    include_hashed_step=False. ingestion_to_source is dropped if
    skip_ingestion=True - e.g. source files are already sitting in
    SOURCE_S3_BUCKET from a prior run, so there's no need to re-hit the
    data.gov.sg API (and its occasional 429 rate-limit retries) again."""
    if only_step:
        if only_step not in ALL_STEPS:
            raise ValueError(f"Unknown only_step {only_step!r} - expected one of {ALL_STEPS}")
        print(f"only_step={only_step!r} - running ONLY this step, skipping the rest of the chain\n")
        return [only_step]

    steps = list(BASE_PIPELINE_STEPS)
    if include_hashed_step:
        steps.append("hashed_iceberg")
    if skip_ingestion and "ingestion_to_source" in steps:
        steps.remove("ingestion_to_source")
        print("skip_ingestion=True - skipping ingestion_to_source (source files already exist)\n")
    return steps


# --------------------------------------------------------------------------- #
# Step execution - one function per run_mode
# --------------------------------------------------------------------------- #

def run_glue_job(job_name: str, glue_client) -> dict:
    """Start a Glue job run and block until it reaches a terminal state."""
    print(f"Starting Glue job: {job_name}")
    start_resp = glue_client.start_job_run(JobName=job_name)
    run_id = start_resp["JobRunId"]

    # Polls every GLUE_POLL_INTERVAL_SECONDS (15s) - a job that runs for
    # several minutes would otherwise print the same "state=RUNNING" line
    # dozens of times over. Only print when the state actually CHANGES, so
    # the log still shows every real transition (STARTING -> RUNNING ->
    # SUCCEEDED/FAILED) without the repeated no-op lines in between.
    last_printed_state = None
    while True:
        status = glue_client.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        state = status["JobRunState"]
        if state != last_printed_state:
            print(f"  [{job_name}] run_id={run_id} state={state}")
            last_printed_state = state
        if state in TERMINAL_STATES:
            return status
        time.sleep(GLUE_POLL_INTERVAL_SECONDS)


def run_local_step(step_name: str) -> dict:
    """Call the matching job's main() directly in-process - no metadata
    Lambda call happens here any more (that used to be optional via a
    track_context flag; now it never happens per-step, since
    run_pipeline() does ONE consolidated metadata update for the whole run
    instead - see this module's docstring). Still times the step and
    captures a record count locally (pure Python, no AWS call) so the
    end-of-run alert table has real numbers per step.

    Returns a dict shaped like the bits of a Glue job-run status this
    module actually reads (JobRunState/ErrorMessage/NumberOfRecords/
    DurationSeconds), so run_step_or_alert() can treat local and glue mode
    identically."""
    print(f"Running locally: {step_name}")
    started = time.monotonic()
    try:
        if step_name == "ingestion_to_source":
            import job_1_ingestion_to_source as job
        elif step_name == "raw_iceberg":
            import job_2_raw_iceberg as job
        elif step_name == "data_profiling":
            import job_2b_data_profiling as job
        elif step_name == "cleaned_iceberg":
            import job_3_cleaned_iceberg as job
        elif step_name == "transformed_iceberg":
            import job_4_transformed_iceberg as job
        elif step_name == "hashed_iceberg":
            import job_5_hashed_iceberg as job
        else:
            raise ValueError(f"Unknown step: {step_name}")

        result = job.main()
        duration = time.monotonic() - started
        records = len(result) if hasattr(result, "__len__") else 0
        print(f"  [{step_name}] SUCCEEDED")
        return {"JobRunState": "SUCCEEDED", "NumberOfRecords": records, "DurationSeconds": duration}

    except Exception as exc:
        duration = time.monotonic() - started
        print(f"  [{step_name}] FAILED: {exc}")
        return {"JobRunState": "FAILED", "ErrorMessage": str(exc), "NumberOfRecords": 0, "DurationSeconds": duration}


# send_alert() lives in common.py (builds the SNS topic ARN at call time
# from the live account id via get_account_id() - never hardcoded/stored,
# see that module's docstring) - imported above rather than duplicated
# here, matching this file's own "exactly one place" principle.


def _format_run_table(run_log: list) -> str:
    """Plain-text aligned table of every step's outcome - readable in an
    SNS email (plain text, no HTML/markdown rendering) or a terminal."""
    if not run_log:
        return "(no steps ran)"

    header = f"{'Step':<22} {'Status':<10} {'Records':>10} {'Duration(s)':>12}  Error"
    rule = "-" * len(header)
    lines = [header, rule]
    for entry in run_log:
        records = entry.get("records")
        records_str = f"{records:,}" if isinstance(records, int) else "-"
        duration = entry.get("duration")
        duration_str = f"{duration:.1f}" if isinstance(duration, (int, float)) else "-"
        error = entry.get("error") or "-"
        if len(error) > 60:
            error = error[:57] + "..."
        lines.append(f"{entry['step']:<22} {entry['state']:<10} {records_str:>10} {duration_str:>12}  {error}")
    return "\n".join(lines)


def run_step_or_alert(step_name: str, run_log: list, run_mode: str, glue_client=None) -> bool:
    """Runs one step (glue or local, based on run_mode) and appends a
    detailed entry to run_log. No longer sends its own alert - the whole
    run gets exactly ONE alert at the end (see run_pipeline()), covering
    every step that ran, not just the one that failed."""
    if run_mode == "glue":
        job_name = GLUE_JOB_NAMES[step_name]
        result = run_glue_job(job_name, glue_client)
    else:
        job_name = f"local:{step_name}"
        result = run_local_step(step_name)

    state = result.get("JobRunState", "UNKNOWN")
    run_log.append({
        "step": step_name,
        "job": job_name,
        "state": state,
        "records": result.get("NumberOfRecords"),
        # Glue's own get_job_run() response uses "ExecutionTime" (seconds);
        # local mode uses "DurationSeconds" - fall back across both names.
        "duration": result.get("DurationSeconds", result.get("ExecutionTime")),
        "error": result.get("ErrorMessage"),
    })
    return state == "SUCCEEDED"


# --------------------------------------------------------------------------- #
# The whole chain
# --------------------------------------------------------------------------- #

def run_pipeline(run_mode: str = "local", include_hashed_step: bool = True, skip_ingestion: bool = False,
                  track_context: bool = True, only_step: str = None):
    """Runs every step in order, stopping at the first failure. Sends
    exactly ONE SNS alert at the end either way (success or failure), with
    a detailed per-step table (status/records/duration/error) - not a
    separate alert per step. Returns (pipeline_succeeded, run_log).

    only_step='cleaned_iceberg' (etc.) - run just that ONE step, for
    debugging a single stage without re-running everything before it. See
    build_pipeline_steps()'s docstring for the details/caveats.

    track_context=True (default) does ONE consolidated metadata Lambda
    update for the WHOLE run - one pipeline_runs row (layer="pipeline", or
    the step's own name if only_step= made this a single-step run), and a
    table_watermarks bump ONLY if that table's load_type is NOT 'FULL'
    (full-load tables truncate and reload every run, so there's no
    meaningful watermark to track - see common.py's overwrite_iceberg() and
    context_tracking.py's run_step_with_context()). Set track_context=False
    to skip metadata tracking entirely, e.g. if the Lambda isn't deployed."""
    assert run_mode in ("local", "glue"), f"run_mode must be 'local' or 'glue', got {run_mode!r}"

    steps = build_pipeline_steps(include_hashed_step, skip_ingestion, only_step=only_step)
    glue_client = boto3.client("glue", region_name=AWS_REGION) if run_mode == "glue" else None

    run_log = []
    pipeline_succeeded = True
    failed_step = None
    pipeline_start = datetime.utcnow()

    for step in steps:
        if not run_step_or_alert(step, run_log, run_mode, glue_client):
            pipeline_succeeded = False
            failed_step = step
            break

    pipeline_end = datetime.utcnow()

    if track_context:
        # ONE metadata read + ONE metadata write for the ENTIRE run above -
        # not one per step. layer is the single step's own name when this
        # was an only_step= debug run (clearer in pipeline_runs than the
        # generic "pipeline" label), otherwise "pipeline".
        layer_label = steps[0] if len(steps) == 1 else "pipeline"
        context = create_context()
        target_table_id = resolve_target_table_id(context)
        next_run_id = compute_next_run_id(context)

        status = "SUCCEEDED" if pipeline_succeeded else "FAILED"
        error_message = None
        if not pipeline_succeeded:
            failed_entry = next((e for e in run_log if e["step"] == failed_step), None)
            error_message = f"Failed at step '{failed_step}': {failed_entry.get('error') if failed_entry else 'unknown error'}"

        load_type = get_table_parameter(target_table_id, "load_type", default="FULL").strip().upper()
        watermark_updates = []
        if pipeline_succeeded and load_type != "FULL":
            watermark_updates = [{
                "table_id": target_table_id,
                "last_watermark_value": pipeline_end.isoformat(),
                "last_run_id": next_run_id,
            }]

        total_records = sum(e.get("records") or 0 for e in run_log)

        context = update_context(
            watermark_updates=watermark_updates,
            pipeline_run={
                "run_id": next_run_id,
                "table_id": target_table_id,
                "layer": layer_label,
                "start_time": pipeline_start.isoformat(),
                "end_time": pipeline_end.isoformat(),
                "status": status,
                "number_of_records": total_records,
                "error_message": error_message,
            },
            path=DEFAULT_CONTEXT_PATH,
        )
        print(f"context updated (consolidated, whole run) - run_id={next_run_id}, table_id={target_table_id}, status={status}")
        verify_context_update(target_table_id, next_run_id, watermark_updates, path=DEFAULT_CONTEXT_PATH)

    table_str = _format_run_table(run_log)
    total_records = sum(e.get("records") or 0 for e in run_log)
    subject = "HDB pipeline SUCCEEDED" if pipeline_succeeded else f"HDB pipeline FAILED at step: {failed_step}"
    message = (
        f"Mode: {run_mode}\n"
        f"Overall status: {'SUCCEEDED' if pipeline_succeeded else 'FAILED'}\n"
        f"Steps run: {len(run_log)}   Total records: {total_records:,}\n\n"
        f"{table_str}\n"
    )
    send_alert(subject=subject, message=message)

    print("\nFinal run log:")
    print(table_str)

    return pipeline_succeeded, run_log
