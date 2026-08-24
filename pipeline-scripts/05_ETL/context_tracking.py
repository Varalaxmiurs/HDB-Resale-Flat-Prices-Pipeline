"""
context_tracking.py
====================
Generic "read context -> run a pipeline step -> update context -> verify"
wrapper, built on top of metadata_lambda.py's create_context()/
update_context(). Originally written as one-off code just for job_1
(ingestion_to_source) in the notebook; pulled out here so the SAME wrapper
is reused for every step - raw_iceberg, cleaned_iceberg, transformed_iceberg,
hashed_iceberg - instead of being copy-pasted per job. orchestration.py's
run_local_step() calls this for every step in the pipeline.

NOTE on "failed" records: job_3/4/5 route rejected rows to failed_iceberg
internally (route_to_failed(), inside their own main()) - that's not a
separate orchestration step with its own table_id/run_id here, it's part
of whichever job's run produced them, so it's covered under that job's
context update (e.g. layer="cleaned_iceberg"), not a standalone entry.

Usage:

    from context_tracking import run_step_with_context

    result = run_step_with_context(job_main=job_1_ingestion_to_source.main, layer="ingestion_to_source")
    # result["status"]  -> "SUCCEEDED" | "FAILED"
    # result["context"] -> the fresh context after update + verify
    # raises if job_main() itself raised, AFTER logging the failure to context
"""

import datetime

from common import get_table_parameter
from metadata_lambda import DEFAULT_CONTEXT_PATH, create_context, update_context

# Every layer gets its OWN context snapshot file (context_raw_iceberg.json,
# context_cleaned_iceberg.json, ...) instead of every layer's read/update
# overwriting the SAME shared context.json - previously, by the time the
# whole pipeline finished, context.json only ever reflected whichever layer
# ran LAST (hashed_iceberg), because every create_context()/update_context()
# call in between had already been clobbered by the next layer's write.
# create_context()/update_context() already take a `path` kwarg for exactly
# this - this just computes the right one per layer and passes it through
# consistently everywhere a fresh read happens for that layer's step.
CONTEXT_DIR = DEFAULT_CONTEXT_PATH.parent


def _context_path_for_layer(layer: str):
    return CONTEXT_DIR / f"context_{layer}.json"


def resolve_target_table_id(context: dict, table_name: str = "resaleflat_price") -> int:
    """Looks up metadata_tables for table_name's table_id - shared by
    run_step_with_context() (per-layer tracking) and orchestration.py's
    run_pipeline() (one consolidated tracking call for the whole run),
    so this lookup lives in exactly one place."""
    metadata_rows = context["tables"].get("metadata_tables", [])
    target_row = next((r for r in metadata_rows if r.get("table_name") == table_name), None)
    return int(target_row["table_id"]) if target_row else 1


def compute_next_run_id(context: dict) -> int:
    """Next pipeline_runs.run_id = 1 + the highest run_id already on record.
    Shared for the same reason as resolve_target_table_id() above."""
    existing_run_ids = [
        int(r["run_id"]) for r in context["tables"].get("pipeline_runs", [])
        if r.get("run_id") is not None
    ]
    return max(existing_run_ids, default=0) + 1


def run_step_with_context(job_main, layer: str, table_name: str = "resaleflat_price") -> dict:
    """
    1) READ current context.
    2) RUN job_main() - the actual job's main() function, whatever it is.
    3) UPDATE context with what really happened (success/fail, timing,
       record count IF job_main returned something with a length - job_1
       returns a list of source paths; job_2..job_5 return None since they
       already call common.py's record_audit() internally for row counts,
       so number_of_records is 0 for those here - that's expected, not a bug).
    4) VERIFY - re-read context fresh and confirm the update landed.

    The context dict itself is threaded through in memory as a normal
    Python variable throughout (context = create_context(); ... context =
    update_context(...)) - the JSON file is just a secondary on-disk copy
    for inspecting after the fact, written to a PER-LAYER path (see
    _context_path_for_layer()) so each stage's snapshot survives the next
    stage's run instead of being overwritten by it.

    Re-raises job_main()'s exception on failure (after logging it to
    context) so callers - including orchestration.py's run_local_step(),
    which wraps this in its own try/except - still see the step as failed.
    """
    context_path = _context_path_for_layer(layer)
    context = create_context(path=context_path)

    target_table_id = resolve_target_table_id(context, table_name)
    next_run_id = compute_next_run_id(context)

    start_time = datetime.datetime.utcnow()
    try:
        result = job_main()
        end_time = datetime.datetime.utcnow()
        status = "SUCCEEDED"
        error_message = None
        number_of_records = len(result) if hasattr(result, "__len__") else 0
        print(f"{layer} SUCCEEDED" + (f" - {number_of_records} record(s)" if hasattr(result, "__len__") else ""))
    except Exception as exc:
        end_time = datetime.datetime.utcnow()
        status = "FAILED"
        error_message = str(exc)
        number_of_records = 0
        print(f"{layer} FAILED: {exc}")

    duration_seconds = (end_time - start_time).total_seconds()

    # Watermarks only mean anything for INCREMENTAL loads ("last point we
    # successfully processed up to") - a FULL-load table (this pipeline's
    # default, see common.py's overwrite_iceberg()) truncates and reloads
    # everything every run, so there's no meaningful "how far did we get"
    # to track. Gate on table_parameters.load_type (the same metadata-driven
    # switch write_by_load_type() uses) rather than bumping the watermark
    # unconditionally on every success.
    load_type = get_table_parameter(target_table_id, "load_type", default="FULL").strip().upper()
    watermark_updates = []
    if status == "SUCCEEDED" and load_type != "FULL":
        watermark_updates = [{
            "table_id": target_table_id,
            "last_watermark_value": end_time.isoformat(),
            "last_run_id": next_run_id,
        }]

    context = update_context(
        watermark_updates=watermark_updates,
        pipeline_run={
            "run_id": next_run_id,
            "table_id": target_table_id,
            "layer": layer,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": status,
            "number_of_records": number_of_records,
            "error_message": error_message,
        },
        path=context_path,
    )
    print(f"context updated - run_id={next_run_id}, table_id={target_table_id}, status={status}")

    verify_context_update(target_table_id, next_run_id, watermark_updates, path=context_path)

    if status == "FAILED":
        # Attach the numbers we already computed (records/duration) to the
        # exception itself before raising - orchestration.py's
        # run_local_step() reads them back off the exception via getattr()
        # so the alert table can show a row for a FAILED step too, not just
        # successful ones. See that function's docstring for why this
        # raises instead of returning normally.
        exc_to_raise = RuntimeError(error_message)
        exc_to_raise.number_of_records = number_of_records
        exc_to_raise.duration_seconds = duration_seconds
        raise exc_to_raise

    return {
        "status": status,
        "context": context,
        "target_table_id": target_table_id,
        "run_id": next_run_id,
        "watermark_updates": watermark_updates,
        "error_message": error_message,
        "number_of_records": number_of_records,
        "duration_seconds": duration_seconds,
    }


def verify_context_update(target_table_id: int, run_id: int, watermark_updates: list, path=DEFAULT_CONTEXT_PATH) -> bool:
    """Re-reads context FRESH (a brand new Lambda "read" call, not reusing
    update_context()'s own response) and asserts the new pipeline_runs row,
    and (only on success) the bumped watermark, are really there. Raises
    AssertionError if not - this is the "prove it, don't just trust it"
    check, same as the original notebook cell.

    path defaults to the shared context.json for standalone/ad-hoc callers
    (e.g. a bash test script), but run_step_with_context() always passes
    its own per-layer path so this verify read lands in the SAME file as
    that layer's create_context()/update_context() calls, not the shared
    default."""
    fresh = create_context(path=path)

    watermark_row = next(
        (w for w in fresh["tables"].get("table_watermarks", []) if str(w.get("table_id")) == str(target_table_id)),
        None,
    )
    run_row = next(
        (r for r in fresh["tables"].get("pipeline_runs", []) if str(r.get("run_id")) == str(run_id)),
        None,
    )

    assert run_row is not None, f"pipeline_runs has no row for run_id={run_id}"
    print(f"OK  pipeline_runs contains run_id={run_id} (status={run_row.get('status')})")

    if watermark_updates:
        expected = watermark_updates[0]["last_watermark_value"]
        assert watermark_row is not None, f"table_watermarks has no row for table_id={target_table_id}"
        assert watermark_row.get("last_watermark_value") == expected, (
            f"table_watermarks[table_id={target_table_id}] did not reflect the new watermark - "
            f"got {watermark_row.get('last_watermark_value')!r}, expected {expected!r}"
        )
        print(f"OK  table_watermarks[table_id={target_table_id}] advanced to {watermark_row['last_watermark_value']}")
    else:
        print("Watermark intentionally left unchanged (step failed, or this table's load_type is FULL - "
              "full-load tables truncate and reload every run, so there's no meaningful watermark to advance).")

    print("CONTEXT UPDATE TEST PASSED")
    return True
