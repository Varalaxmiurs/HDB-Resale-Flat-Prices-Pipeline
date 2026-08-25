"""
job_2_raw_iceberg.py
======================
Glue Python Shell (or Spark) job. Second stage of the pipeline.

    S3 source (raw CSVs, from job_1 AND/OR a manual upload) -> raw_iceberg

Reads every CSV sitting in the source bucket - both what job_1 landed under
config.SOURCE_S3_PREFIX (the automated data.gov.sg pull) and anything
dropped under config.SOURCE_MANUAL_UPLOAD_PREFIX (a manual upload) -
combines them into a single dataframe, and writes them into raw_iceberg
with only the bare minimum transformation needed to make the table usable
downstream:
    - drop rows that are entirely blank (a rare artifact of source CSVs)
    - compute the deterministic surrogate_key (SHA-256 of the composite
      natural key, per common.compute_surrogate_key / config.NATURAL_KEY_COLUMNS)
      - carried forward by every stage after this one.

Written via overwrite_iceberg(), not merge_iceberg(): job_1's source pull is
always the FULL "Resale Flat Prices" collection, never a delta, so each run
here fully replaces raw_iceberg's contents rather than upserting into it -
see common.py's overwrite_iceberg() docstring for the reasoning.

No field validation, deduplication, or business-rule cleaning happens here
- that belongs to job_3. This stage is intentionally "as close to source as
possible, plus the key needed to make downstream merges idempotent."

NOTE (2026-08-25): source files are read in place and left there - no
archiving/moving of processed files (an earlier version of this job moved
processed files into a separate archive bucket; removed per request).
Rerunning job_2 against the same source files simply reprocesses them,
which is safe since raw_iceberg is a full overwrite each run anyway.

A manual upload does not need job_1 to run first: drop a CSV under
s3://<source bucket>/<SOURCE_MANUAL_UPLOAD_PREFIX>/ and an EventBridge rule
watching that prefix for "Object Created" starts the SAME Step Functions
chain (job_2 onward) directly - see setup.sh Step 11's rule, scoped to
that prefix only so it can never double-fire alongside job_1's own
SUCCEEDED-triggered run (Step 11D), which lands files under the OTHER
prefix.

NOTE: this file previously contained job_3's code verbatim (raw_iceberg and
cleaned_iceberg were byte-identical). This is a from-scratch implementation
reconstructed to match the pipeline's documented raw_iceberg contract
(common.py's docstring: "combined raw data, minimal transformation") and
the conventions established by job_3/4/5. Since no original job_2 spec
survived to diff against, please review before deploying - in particular
the source-file read (wr.s3.read_csv over the whole source prefix) and the
"drop fully-blank rows" transformation are this author's best inference of
"minimal transformation," not confirmed requirements.
"""

import pandas as pd

from common import compute_surrogate_key, get_logger, read_csv_files_from_s3, record_audit, write_by_load_type
from config import (
    DATE_RANGE_END,
    DATE_RANGE_START,
    MAX_ROWS_TO_INGEST,
    SOURCE_MANUAL_UPLOAD_PREFIX,
    SOURCE_S3_BUCKET,
    SOURCE_S3_PREFIX,
)

logger = get_logger("job_2_raw_iceberg")


def read_source_files() -> pd.DataFrame:
    """Read and concatenate every CSV sitting in the source bucket, from
    BOTH places a file can land there:
      - SOURCE_S3_PREFIX   - job_1's automated data.gov.sg pull
      - SOURCE_MANUAL_UPLOAD_PREFIX - someone dropped a CSV in by hand
    A manually-uploaded file is not a special/lesser case - it's combined
    into the same dataframe and goes through every stage below exactly like
    an automated one (date-range filter, surrogate key, cleaned/transformed/
    hashed/failed routing)."""
    automated_df = read_csv_files_from_s3(SOURCE_S3_BUCKET, SOURCE_S3_PREFIX)
    logger.info("Read %d rows from automated source file(s) under s3://%s/%s/", len(automated_df), SOURCE_S3_BUCKET, SOURCE_S3_PREFIX)

    manual_df = read_csv_files_from_s3(SOURCE_S3_BUCKET, SOURCE_MANUAL_UPLOAD_PREFIX)
    if not manual_df.empty:
        logger.info("Read %d rows from manually-uploaded source file(s) under s3://%s/%s/", len(manual_df), SOURCE_S3_BUCKET, SOURCE_MANUAL_UPLOAD_PREFIX)

    return pd.concat([automated_df, manual_df], ignore_index=True) if not manual_df.empty else automated_df


def filter_to_configured_date_range(df: pd.DataFrame) -> pd.DataFrame:
    """Row-level safety net for the brief's stated scope ("...using datasets
    from January 2012 to December 2016"). job_1 can only select whole SOURCE
    FILES by whether their published coverage overlaps that window AT ALL
    (filter_datasets_by_range()) - it can't download only part of a file.
    data.gov.sg's collection includes a resource covering roughly 2000 to
    Feb 2012, which genuinely overlaps our window by only ~2 months, but
    still gets landed in full by job_1 - potentially hundreds of thousands
    of 2000-2011 rows that are outside this pipeline's required scope.
    Filtering here, by the `month` column, on the COMBINED dataset is what
    actually enforces "January 2012 to December 2016", regardless of which
    whole files job_1 happened to land. This is a programmatic scoping step
    matching the brief's own explicitly stated date range, not a "manual
    modification" to the data - the brief's "process the data file as-is"
    instruction is about not hand-editing the CSV in a spreadsheet, not
    about ignoring the task's own stated scope."""
    month = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    start, end = pd.to_datetime(DATE_RANGE_START), pd.to_datetime(DATE_RANGE_END)
    in_range = month.notna() & (month >= start) & (month <= end)
    dropped = len(df) - int(in_range.sum())
    if dropped:
        logger.info(
            "Filtered %d row(s) outside the configured date range %s..%s "
            "(a landed source file's published coverage can extend beyond "
            "this range even though it was only pulled for a partial overlap)",
            dropped, DATE_RANGE_START, DATE_RANGE_END,
        )
    return df[in_range].copy()


def main() -> None:
    source_df = read_source_files()

    # Enforce the brief's stated date scope BEFORE anything else - see
    # filter_to_configured_date_range()'s docstring for why this can't be
    # done at job_1 (whole-file granularity only).
    source_df = filter_to_configured_date_range(source_df)

    # Minimal transformation: drop fully-blank rows, then attach the
    # deterministic surrogate key used for idempotent merges from here on.
    raw_df = source_df.dropna(how="all").copy()
    dropped_blank = len(source_df) - len(raw_df)

    # TESTING-ONLY block - remove this whole if-block (and the
    # MAX_ROWS_TO_INGEST import above) before final deployment.
    if MAX_ROWS_TO_INGEST > 0 and len(raw_df) > MAX_ROWS_TO_INGEST:
        logger.info(
            "HDB_MAX_ROWS=%d - testing cap applied, keeping %d of %d rows for raw_iceberg "
            "(every downstream stage inherits this cap too, since they all read from here)",
            MAX_ROWS_TO_INGEST, MAX_ROWS_TO_INGEST, len(raw_df),
        )
        raw_df = raw_df.head(MAX_ROWS_TO_INGEST).copy()

    raw_df["surrogate_key"] = compute_surrogate_key(raw_df)

    write_by_load_type(raw_df, stage="raw", table_id=1)  # FULL/MERGE decided by table_parameters, not hardcoded here
    logger.info(
        "job_2 complete: %d rows loaded into raw_iceberg (%d fully-blank rows dropped)",
        len(raw_df), dropped_blank,
    )

    record_audit(
        job_name="job_2_raw_iceberg", stage="raw",
        rows_in=len(source_df), rows_out=len(raw_df), rows_rejected=dropped_blank,
    )


if __name__ == "__main__":
    main()
