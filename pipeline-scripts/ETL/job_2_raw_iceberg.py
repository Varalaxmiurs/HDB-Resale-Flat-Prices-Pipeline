"""
job_2_raw_iceberg.py
======================
Glue Python Shell (or Spark) job. Second stage of the pipeline.

    S3 source (raw CSVs, as landed by job_1) -> raw_iceberg

Reads every CSV file job_1 landed under the source prefix, combines them
into a single dataframe, and writes them into raw_iceberg with only the
bare minimum transformation needed to make the table usable downstream:
    - drop rows that are entirely blank (a rare artifact of source CSVs)
    - compute the deterministic surrogate_key (SHA-256 of the composite
      natural key, per common.compute_surrogate_key / config.NATURAL_KEY_COLUMNS)
      so raw_iceberg - and every stage after it - can be idempotently
      upserted via merge_iceberg() on rerun.

No field validation, deduplication, or business-rule cleaning happens here
- that belongs to job_3. This stage is intentionally "as close to source as
possible, plus the key needed to make downstream merges idempotent."

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

import awswrangler as wr
import pandas as pd

from common import compute_surrogate_key, get_logger, merge_iceberg, record_audit
from config import SOURCE_S3_BUCKET, SOURCE_S3_PREFIX

logger = get_logger("job_2_raw_iceberg")


def read_source_files() -> pd.DataFrame:
    """Read and concatenate every CSV job_1 landed under the source prefix."""
    path = f"s3://{SOURCE_S3_BUCKET}/{SOURCE_S3_PREFIX}/"
    df = wr.s3.read_csv(path=path, path_suffix=".csv", dataset=False)
    logger.info("Read %d rows from source files under %s", len(df), path)
    return df


def main() -> None:
    source_df = read_source_files()

    # Minimal transformation: drop fully-blank rows, then attach the
    # deterministic surrogate key used for idempotent merges from here on.
    raw_df = source_df.dropna(how="all").copy()
    dropped_blank = len(source_df) - len(raw_df)
    raw_df["surrogate_key"] = compute_surrogate_key(raw_df)

    merge_iceberg(raw_df, stage="raw")  # idempotent upsert on surrogate_key
    logger.info(
        "job_2 complete: %d rows merged into raw_iceberg (%d fully-blank rows dropped)",
        len(raw_df), dropped_blank,
    )
    record_audit(
        job_name="job_2_raw_iceberg", stage="raw",
        rows_in=len(source_df), rows_out=len(raw_df), rows_rejected=dropped_blank,
    )


if __name__ == "__main__":
    main()
