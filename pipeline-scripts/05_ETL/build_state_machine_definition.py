"""
build_state_machine_definition.py
====================================
Generates the Amazon States Language (ASL) JSON for the HDB pipeline's
Step Functions state machine. Shared by setup.sh (which calls this at
deploy time) and the CI/CD workflow (which regenerates + pushes an
update whenever this file or setup.sh's job/topic/lambda names change) -
one place owns this definition, not two copies drifting apart.

Usage:
    python build_state_machine_definition.py \
        <sns_topic_arn> <lambda_arn> \
        <glue_job_2> <glue_job_2b> <glue_job_3> <glue_job_4> <glue_job_5> \
        <output_path>

Ingestion (job1) is deliberately NOT a state here - see setup.sh's Step
11C/11D comments for why (it runs standalone; its own Glue Job SUCCEEDED
event is what starts this state machine, via an EventBridge rule).

Every job step, on failure, records the failure into pipeline_runs (via
the metadata-reader Lambda) before alerting - so the metadata tables and
the SNS notification always agree on what happened. On success, the run
summary that gets written to pipeline_runs is echoed back into the
success email so it's the same numbers, twice-verified (read before,
written+read after).

SNS message bodies are built with States.Format() so they read as a
short labelled report instead of a raw JSON error dump. Only ONE dynamic
value is interpolated per message ($.error.Cause for failures, or the
run summary table for success) - everything else (title, step name,
job name) is a plain string baked in at build time. This is deliberate:
Step Functions can only Catch failures on Task states, not on a Pass
state's own Parameters evaluation, so parsing $.error.Cause (a JSON
string) into individual fields via States.StringToJson inside a Pass
state would have no safety net if a future Glue failure ever produced a
non-JSON Cause - the whole run would die with no notification sent at
all. Keeping the raw Cause as a single clearly-labelled block is the
version that can never fail to notify.
"""

import json
import sys

(sns_topic_arn, lambda_arn,
 job2, job2b, job3, job4, job5,
 out_path) = sys.argv[1:]

# (state_name, glue_job_name, pipeline_runs "layer" value, human label, next state on success)
STEPS = [
    ("Job2_RawIceberg",         job2,  "job_2_raw_iceberg",         "raw_iceberg",         "Job2b_DataProfiling"),
    ("Job2b_DataProfiling",     job2b, "job_2b_data_profiling",     "data_profiling",      "Job3_CleanedIceberg"),
    ("Job3_CleanedIceberg",     job3,  "job_3_cleaned_iceberg",     "cleaned_iceberg",     "Job4_TransformedIceberg"),
    ("Job4_TransformedIceberg", job4,  "job_4_transformed_iceberg", "transformed_iceberg", "Job5_HashedIceberg"),
    ("Job5_HashedIceberg",      job5,  "job_5_hashed_iceberg",      "hashed_iceberg",      "WriteContext"),
]

states = {
    "ReadContextBefore": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
        "ResultPath": "$.context_before",
        "Next": STEPS[0][0],
    },
}

for state_name, job_name, layer, label, next_name in STEPS:
    write_failed_name = f"WriteFailedRun_{state_name}"
    notify_name = f"NotifyFailure_{state_name}"

    states[state_name] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": {"JobName": job_name},
        "Catch": [{
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.error",
            "Next": write_failed_name,
        }],
        "Next": next_name,
    }

    # Record the failure in pipeline_runs BEFORE alerting, so metadata and
    # the email always agree. If even this write fails (Lambda/Athena
    # trouble on top of the original Glue failure), still fall through to
    # the alert rather than dying silently - the email is the backstop.
    states[write_failed_name] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
            "FunctionName": lambda_arn,
            "Payload": {
                "action": "update",
                "pipeline_run": {
                    "run_id.$": "States.MathRandom(1, 999999999)",
                    "table_id": 1,
                    "layer": layer,
                    "start_time.$": "$$.State.EnteredTime",
                    "end_time.$": "$$.State.EnteredTime",
                    "status": "FAILED",
                    "error_message.$": "$.error.Cause",
                },
            },
        },
        "ResultPath": "$.write_failed_result",
        "Catch": [{
            "ErrorEquals": ["States.ALL"],
            "ResultPath": "$.write_failed_error",
            "Next": notify_name,
        }],
        "Next": notify_name,
    }

    # Nicely-labelled failure report. $.error.Cause is the one dynamic
    # value (the raw Glue/Lambda error detail) - everything else is a
    # plain string known at build time, so this Format call can't itself
    # fail regardless of what shape Cause turns out to be.
    states[notify_name] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::sns:publish",
        "Parameters": {
            "TopicArn": sns_topic_arn,
            "Subject": "HDB Pipeline Run - FAILURE",
            "Message.$": (
                "States.Format('"
                "HDB Resale Flat Prices Pipeline - Run FAILED\n"
                "=============================================\n"
                f"Step failed : {label} ({job_name})\n"
                f"pipeline_runs updated : layer={layer}, status=FAILED\n\n"
                "Failure details:\n"
                "{}"
                "', $.error.Cause)"
            ),
        },
        "Next": "PipelineFailed",
    }

states["WriteContext"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {
        "FunctionName": lambda_arn,
        "Payload": {
            "action": "update",
            "sync_pipeline_runs_from_audit": {
                "run_id.$": "States.MathRandom(1, 999999999)",
                "since.$": "$$.Execution.StartTime",
                "table_id": 1,
            },
        },
    },
    "ResultPath": "$.write_context_result",
    "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.write_context_error",
        "Next": "ReadContextAfter",
    }],
    "Next": "ReadContextAfter",
}

states["ReadContextAfter"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
    "ResultPath": "$.context_after",
    "Next": "NotifySuccess",
}

# Nicely-labelled success report. The one dynamic value is the run summary
# table that WriteContext just wrote (and that ReadContextAfter re-read),
# so the email always reflects genuinely new state, not a stale re-read.
states["NotifySuccess"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::sns:publish",
    "Parameters": {
        "TopicArn": sns_topic_arn,
        "Subject": "HDB Pipeline Run - SUCCESS",
        "Message.$": (
            "States.Format('"
            "HDB Resale Flat Prices Pipeline - ETL process completed successfully\n"
            "=====================================================================\n"
            "All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, "
            "transformed, hashed.\n\n"
            "Run summary (written to pipeline_runs this run):\n"
            "{}\n\n"
            "Metadata was read before this run and read again after a real write "
            "to pipeline_runs (via sync_pipeline_runs_from_audit) - the table "
            "above reflects genuinely new state, not just two identical reads."
            "', $.write_context_result.Payload.run_summary_table)"
        ),
    },
    "Next": "PipelineSucceeded",
}

states["PipelineSucceeded"] = {"Type": "Succeed"}
states["PipelineFailed"] = {"Type": "Fail"}

definition = {
    "Comment": (
        "HDB Resale Flat Prices pipeline - runs each Glue job in order, "
        "stops and alerts on first failure, verifies before/after "
        "metadata on success."
    ),
    "StartAt": "ReadContextBefore",
    "States": states,
}

with open(out_path, "w") as f:
    json.dump(definition, f, indent=2)
