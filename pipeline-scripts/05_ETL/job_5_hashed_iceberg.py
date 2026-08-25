"""
job_5_hashed_iceberg.py
=========================
Glue Python Shell (or Spark) job. Fifth stage of the pipeline.

    transformed_iceberg -> hashed_iceberg (+ failed_iceberg for rejects)

Implements the hashing requirement: SHA-256 hash of resale_identifier,
irreversible. That's it - no SCD Type 2 versioning on top of it.

SCD2 REMOVED (2026-08-25): earlier versions of this file kept a full
slowly-changing-dimension history here (row_hash/version/effective_start_
date/effective_end_date/is_current/scd_key columns, a surrogate_key that
was no longer unique once a record changed, a disappeared-keys expiry
pass, etc.). The test brief's actual Data Output Requirements describe
hashed_iceberg as "Cleaned Data + Hashed Identifier Column" - one row per
transaction, not a version history - and never asks for change tracking.
SCD2 was scope creep, not a requirement, and it made this the only stage
in the whole pipeline that wasn't a plain idempotent upsert.

Every transaction is now upserted by surrogate_key via merge_iceberg(),
the SAME pattern job_2/job_3/job_4 already use: a rerun over identical
source data updates the same row in place instead of duplicating it, and
a genuinely changed resale_price or resale_identifier just overwrites the
existing row - no version history kept, hashed_iceberg only ever reflects
the latest state of each transaction.

Only the HASH is kept in the output, not the plaintext resale_identifier
alongside it - keeping both would defeat the point of "hash this column
using an irreversible algorithm" (the brief's own wording), since the
identifier would be trivially recoverable from the very next column over.
"""

import hashlib

from common import get_logger, merge_iceberg, read_iceberg, record_audit, route_to_failed

logger = get_logger("job_5_hashed_iceberg")


def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def main() -> None:
    transformed_df = read_iceberg("transformed")
    logger.info("Read %d rows from transformed_iceberg", len(transformed_df))

    missing_identifier = transformed_df["resale_identifier"].isna() | (transformed_df["resale_identifier"] == "")
    hashable = transformed_df[~missing_identifier].copy()
    unhashable = transformed_df[missing_identifier]
    route_to_failed(unhashable, reason="missing_resale_identifier", stage="hashed")

    # Irreversible hash of the Resale Identifier itself (the original requirement)
    hashable["resale_identifier_hash"] = hashable["resale_identifier"].apply(hash_value)

    # Keep only the hash in the written output - not the plaintext identifier
    # alongside it (see module docstring for why).
    hashable = hashable.drop(columns=["resale_identifier"], errors="ignore")

    # Idempotent upsert by surrogate_key - same pattern as job_2/job_3/job_4.
    # No SCD2 versioning: this MERGE just updates the row in place.
    merge_iceberg(hashable, stage="hashed")

    logger.info(
        "job_5 complete: %d row(s) upserted into hashed_iceberg, %d rejected (missing identifier)",
        len(hashable), len(unhashable),
    )
    record_audit(
        job_name="job_5_hashed_iceberg", stage="hashed",
        rows_in=len(transformed_df), rows_out=len(hashable), rows_rejected=len(unhashable),
    )


if __name__ == "__main__":
    main()
