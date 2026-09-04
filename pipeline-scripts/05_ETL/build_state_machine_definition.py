"""
build_state_machine_definition.py
====================================
Generates the Amazon States Language (ASL) JSON for the HDB pipeline's
Step Functions state machine. Shared by setup.sh (which calls this at
deploy time) and the CI/CD workflow (regenerates + pushes an update
whenever this file or setup.sh's job/topic/lambda names change) - one
place owns this definition, not two copies drifting apart.

Usage:
    python build_state_machine_definition.py \
        <sns_topic_arn> <lambda_arn> \
        <glue_job_compact> \
        <glue_job_2> <glue_job_2b> <glue_job_3> <glue_job_4> <glue_job_5> \
        <ses_sender_email> <ses_recipient_emails> \
        <audit_bucket> \
        <output_path>

Ingestion (job1) is deliberately NOT a state here - it runs standalone;
its own Glue Job SUCCEEDED event is what starts this state machine, via
an EventBridge rule (see setup.sh's Step 11C/11D).

WHY THERE IS NO INLINE Catch ON ANY JOB STEP: every job Task below is a
plain Task -> Next, with no Catch. An earlier version caught every
failure inline (Catch -> write the FAILED row -> send the alert email ->
a dedicated Fail state), which made AWS Step Functions' Redrive feature
useless: Redrive resumes from wherever the execution actually
terminated, and a Caught failure always terminates at our own Fail
state, not the real failing Glue job - so Redriving just re-hit the Fail
state and failed again instantly. Removing the Catch means a real
failure now propagates uncaught and fails the execution AT the real
state, which Redrive can then correctly resume from, skipping every
step that already succeeded.

Recording the failure and alerting still both happen, just OUTSIDE this
execution: setup.sh's Step 11F adds an EventBridge rule watching this
state machine's "Step Functions Execution Status Change" events for
status=FAILED, targeting lambda_function.py directly.
_handle_execution_failure() there calls GetExecutionHistory to find the
real failing state, maps it to a layer via STATE_FAILURE_INFO (kept in
sync with STEPS below by hand), writes the pipeline_runs FAILED row, and
sends the alert email in plain Python - see lambda_function.py's own
docstring.

The success path is unchanged by this design (a successful run was
never the thing needing Redrive): the run summary written to
pipeline_runs is echoed back into the success email so both reflect the
same, freshly-verified numbers.

The success alert (SendSuccessEmail) is OPTIONALLY sent via Amazon SES
instead of SNS - <ses_sender_email>/<ses_recipient_emails> can both be
empty, in which case every Notify state falls back to the original
plain-text sns:publish design, so a deploy with no email configured
still works end-to-end. SES is worth the extra config because SNS's
Email protocol is plain-text only and cannot render an HTML report; SES
has no subscriber list of its own though, so recipients are baked into
the state machine at build time (adding/removing one means rebuilding +
redeploying), and the sender identity (plus every recipient, in SES
sandbox mode) must be verified - see setup.sh's Step 8B. The separate
TIMED_OUT alert (setup.sh's Step 11E) is unaffected either way, since a
hard timeout kills the execution before any Task can run.

The success email's Html/Text bodies are built with States.Format() -
only ONE dynamic value is interpolated ($.write_context_result.Payload.
run_summary_table), everything else is static text baked in at build
time. The FAILURE email is no longer built here at all -
lambda_function.py builds it with plain Python f-strings instead,
sidestepping States.Format's escaping rules entirely.

The Html body uses only inline style="..." attributes, no <style>
block: States.Format() treats "{" and "}" as its own placeholder syntax,
so a CSS block full of braces would need constant hand-escaping, and
most email clients strip <style> blocks anyway. Every Html/Text value
below is also wrapped in a SINGLE-QUOTED States.Format('...') argument,
so the static text must contain zero literal apostrophes - including
inside CSS values (a quoted font-family like 'Courier New' would add
one), which is why the monospace stack is Consolas/Menlo/monospace
(single-word names never need quoting).
"""

import json
import os
import sys

(sns_topic_arn, lambda_arn,
 job_compact, job2, job2b, job3, job4, job5,
 ses_sender_email, ses_recipient_emails,
 audit_bucket,
 out_path) = sys.argv[1:]

SES_RECIPIENTS = [addr.strip() for addr in ses_recipient_emails.split(",") if addr.strip()]
ses_sender_email = ses_sender_email.strip()

USE_SES = bool(SES_RECIPIENTS) and bool(ses_sender_email)

MONO_FONT = "Consolas,Menlo,monospace"

STATE_MACHINE_TIMEOUT_SECONDS = int(
    os.environ.get("HDB_STATE_MACHINE_TIMEOUT_SECONDS", 7200)
)
print(
    f"[build_state_machine_definition] TimeoutSeconds = "
    f"{STATE_MACHINE_TIMEOUT_SECONDS}s "
    f"({STATE_MACHINE_TIMEOUT_SECONDS / 60:.0f} min) - "
    f"{'DEV/TEST override' if 'HDB_STATE_MACHINE_TIMEOUT_SECONDS' in os.environ else 'PRD default'}"
)

VACUUM_MAX_SNAPSHOT_AGE_SECONDS = os.environ.get("HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS", "120")
print(
    f"[build_state_machine_definition] VACUUM retention override = "
    f"{VACUUM_MAX_SNAPSHOT_AGE_SECONDS}s - "
    f"{'DEV/TEST default' if 'HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS' not in os.environ else 'explicit override'}, "
    f"change via HDB_VACUUM_MAX_SNAPSHOT_AGE_SECONDS before a real production deploy."
)
if USE_SES:
    print(
        f"[build_state_machine_definition] Alerts via SES - sender = "
        f"{ses_sender_email}, recipients = {SES_RECIPIENTS}"
    )
else:
    print(
        "[build_state_machine_definition] Alerts via SNS (plain-text) - "
        "no ses_sender_email/ses_recipient_emails given, same as leaving "
        "HDB_ALERT_RECIPIENT_EMAILS unset for Step 8B."
    )

STEPS = [
    ("IngestToRawLayer",         job2,  "job_2_raw_iceberg",         "raw_iceberg",         "ProfileRawData"),
    ("ProfileRawData",     job2b, "job_2b_data_profiling",     "data_profiling",      "CleanDataLayer"),
    ("CleanDataLayer",     job3,  "job_3_cleaned_iceberg",     "cleaned_iceberg",     "TransformDataLayer"),
    ("TransformDataLayer", job4,  "job_4_transformed_iceberg", "transformed_iceberg", "HashAndVersionDataLayer"),
    ("HashAndVersionDataLayer",      job5,  "job_5_hashed_iceberg",      "hashed_iceberg",      "RecordRunSummary"),
]


def _ses_send_email_params(subject: str, html_format_expr: str, text_format_expr: str) -> dict:
    """Shared Parameters shape for arn:aws:states:::aws-sdk:sesv2:sendEmail.
    html_format_expr/text_format_expr are each a full "States.Format(...)"
    string."""
    return {
        "FromEmailAddress": ses_sender_email,
        "Destination": {"ToAddresses": SES_RECIPIENTS},
        "Content": {
            "Simple": {
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data.$": html_format_expr},
                    "Text": {"Data.$": text_format_expr},
                },
            }
        },
    }


def _sns_publish_params(subject: str, message_format_expr: str) -> dict:
    """Shared Parameters shape for arn:aws:states:::sns:publish - the
    original (pre-SES) plain-text design, used as the fallback when no
    SES sender/recipients were configured."""
    return {
        "TopicArn": sns_topic_arn,
        "Subject": subject,
        "Message.$": message_format_expr,
    }


states = {
    "CheckMetadataBeforeRun": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
        "ResultPath": "$.context_before",
        "Next": STEPS[0][0],
    },
}

def _build_job_step(state_name, job_name, next_name, extra_arguments=None, result_path=None):
    """Plain Task -> Next. Deliberately NO Catch here - see the module
    docstring's "Redrive" note for why: a real failure now propagates
    uncaught, failing the execution AT this exact state, which lets Step
    Functions' native Redrive resume from the real failing step instead
    of a downstream Fail state. Recording the failure and alerting still
    happen, just OUTSIDE this execution, via the
    hdb-eventdriven-execution-failure-alert EventBridge rule (setup.sh
    Step 11F) + lambda_function.py's _handle_execution_failure().

    result_path=None (the default, used by every STEPS job) leaves
    ResultPath unset, so ASL's default of "$" applies - the Task's raw
    result REPLACES the entire state input. Harmless for the 5 STEPS
    jobs, but NOT for CompactIcebergTables, which sits between states
    that write $.write_context_result/$.context_after and
    SendSuccessEmail (which reads $.write_context_result.Payload.
    run_summary_table): a default "$" ResultPath there would silently
    wipe both keys with CompactIcebergTables' own result, and
    SendSuccessEmail's States.Format() would then fail outright with
    "could not be found in the input". Passing an explicit result_path
    (e.g. "$.compact_result") merges the Task's result in at that key
    instead of replacing the whole state, so everything written earlier
    survives."""
    params = {"JobName": job_name}
    if extra_arguments:
        params["Arguments"] = extra_arguments
    state = {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": params,
        "Next": next_name,
    }
    if result_path:
        state["ResultPath"] = result_path
    states[state_name] = state


for state_name, job_name, _layer, _label, next_name in STEPS:
    _build_job_step(state_name, job_name, next_name)

states["RecordRunSummary"] = {
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
                "execution_name.$": "$$.Execution.Name",
            },
        },
    },
    "ResultPath": "$.write_context_result",
    "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.write_context_error",
        "Next": "VerifyMetadataAfterRun",
    }],
    "Next": "VerifyMetadataAfterRun",
}

states["VerifyMetadataAfterRun"] = {
    "Type": "Task",
    "Resource": "arn:aws:states:::lambda:invoke",
    "Parameters": {"FunctionName": lambda_arn, "Payload": {"action": "read"}},
    "ResultPath": "$.context_after",
    "Next": "CompactIcebergTables",
}

_build_job_step(
    "CompactIcebergTables", job_compact, "SendSuccessEmail",
    extra_arguments={"--max-snapshot-age-seconds": VACUUM_MAX_SNAPSHOT_AGE_SECONDS},
    result_path="$.compact_result",
)

# NOTE: the markdown/HTML success reports are NOT persisted to S3 from a
# state here anymore - see lambda_function.py's _persist_run_report()
# BUGFIX comment. aws-sdk:s3:putObject's Body parameter is Blob-typed,
# and ANY dynamically-resolved value assigned to it via '.$' - whether
# via States.Format() or a bare JSONPath reference, both were tried and
# both reproduced the same bug in production - gets JSON-encoded by Step
# Functions instead of written as raw bytes. RecordRunSummary's Lambda
# invoke now writes both files directly via boto3's s3_client.put_object()
# itself (passed execution_name.$ above so it can build the same S3 key),
# which sidesteps ASL's Blob-marshalling entirely. SendSuccessEmail is
# unaffected by any of this: sesv2:sendEmail's Content.Simple.Body.Html.
# Data is a plain String-typed field, not Blob, so States.Format() there
# has always been safe.

success_text = (
    "States.Format('"
    "HDB Resale Flat Prices Pipeline - ETL process completed successfully\n"
    "====================================================================\n"
    "All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, "
    "transformed, hashed.\n\n"
    "Run summary (written to pipeline_runs this run):\n"
    "{}\n\n"
    "Metadata was read before this run and read again after a real write "
    "to pipeline_runs (via sync_pipeline_runs_from_audit) - the table "
    "above reflects genuinely new state, not just two identical reads."
    "', $.write_context_result.Payload.run_summary_table)"
)

if USE_SES:
    success_html = (
        "States.Format('"
        "<!doctype html><html lang=\"en\"><head><meta charset=\"UTF-8\"></head>"
        "<body style=\"margin:0;background:#f5f7f9;font-family:Arial,Helvetica,sans-serif;color:#1a2233;\">"
        "<div style=\"max-width:640px;margin:0 auto;padding:32px 20px;\">"
        f"<div style=\"font-family:{MONO_FONT};font-size:12px;letter-spacing:0.05em;text-transform:uppercase;color:#5b6472;margin-bottom:8px;\">HDB Pipeline Run - SUCCESS</div>"
        "<h1 style=\"font-size:22px;font-weight:700;margin:0 0 6px;color:#1a2233;\">Resale Flat Prices ETL - run completed</h1>"
        "<p style=\"color:#5b6472;font-size:14px;margin:0 0 20px;\">All 6 steps ran cleanly: ingestion, raw, data profiling, cleaned, transformed, hashed.</p>"
        "{}"
        "<div style=\"font-size:12px;color:#5b6472;text-align:center;padding-top:6px;\">Metadata was read before this run and read again after a real write to pipeline_runs, so the report above reflects genuinely new state, not two identical reads.</div>"
        "</div></body></html>"
        "', $.write_context_result.Payload.run_summary_html)"
    )
    success_params = _ses_send_email_params(
        "HDB Pipeline Run - SUCCESS", success_html, success_text,
    )
else:
    success_params = _sns_publish_params(
        "HDB Pipeline Run - SUCCESS", success_text,
    )

states["SendSuccessEmail"] = {
    "Type": "Task",
    "Resource": (
        "arn:aws:states:::aws-sdk:sesv2:sendEmail" if USE_SES
        else "arn:aws:states:::sns:publish"
    ),
    "Parameters": success_params,
    "Next": "PipelineCompletedSuccessfully",
}

states["PipelineCompletedSuccessfully"] = {"Type": "Succeed"}

definition = {
    "Comment": (
        "HDB Resale Flat Prices pipeline - runs each Glue job in order, "
        "stops on first failure (uncaught, so it is Redrive-able from the "
        "real failing step), verifies before/after metadata and alerts on "
        "success. Failure alerting happens out-of-band via an EventBridge "
        "rule on this execution's own FAILED status change, not inline."
    ),
    "TimeoutSeconds": STATE_MACHINE_TIMEOUT_SECONDS,
    "StartAt": "CheckMetadataBeforeRun",
    "States": states,
}

with open(out_path, "w") as f:
    json.dump(definition, f, indent=2)
