"""
job_5_hashed_iceberg.py
=========================
Glue Python Shell (or Spark) job. Fifth stage of the pipeline.

    transformed_iceberg -> hashed_iceberg (SCD Type 2, + failed_iceberg for rejects)

Implements the hashing requirement (SHA-256 hash of resale_identifier,
irreversible) AND slowly-changing-dimension history on top of it:

SCD Type 2 design:
    - Business key = `surrogate_key` (already computed in job_2 from the
      composite natural key, and carried unchanged through cleaned and
      transformed). It identifies "this specific resale transaction"
      consistently across reruns.
    - `row_hash` = SHA-256 over the mutable attribute columns we care about
      tracking (resale_price, resale_identifier, remaining_lease_*). Used
      purely to detect whether anything actually changed for a given
      surrogate_key between runs.
    - `scd_key` = SHA-256(surrogate_key + '#' + version) - the TRUE unique
      row identifier for hashed_iceberg, since surrogate_key alone is no
      longer unique across historical versions once a record changes.
    - Standard SCD2 columns: version, effective_start_date,
      effective_end_date (NULL = still current), is_current.

Idempotency: if a rerun sees the exact same data (same surrogate_key, same
row_hash) as the current version, NOTHING is written - no new version, no
duplicate. Only genuinely new or changed rows produce a write. This is what
makes hashed_iceberg safe to rerun, on top of raw/cleaned/transformed's
MERGE-based idempotency.

Extend ATTRIBUTE_COLUMNS_FOR_CHANGE_DETECTION if you want more/fewer
columns to count as "a change" for SCD2 purposes.
"""

import hashlib
from datetime import datetime, timezone

import pandas as pd

from common import athena_read_sql, execute_athena_sql, get_logger, read_iceberg, record_audit, route_to_failed, write_iceberg
from config import ATTRIBUTE_COLUMNS_FOR_CHANGE_DETECTION, GLUE_DATABASE

logger = get_logger("job_5_hashed_iceberg")


def _now_ts() -> str:
    """Athena timestamp literal format: 'YYYY-MM-DD HH:MI:SS.fff'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_row_hash(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in ATTRIBUTE_COLUMNS_FOR_CHANGE_DETECTION if c in df.columns]
    concat = df[cols].astype(str).agg("|".join, axis=1)
    return concat.apply(hash_value)


def read_current_versions() -> pd.DataFrame:
    """SELECT surrogate_key, row_hash, version FROM hashed_iceberg WHERE is_current = true.

    Returns an empty frame (not an error) if the table doesn't exist yet -
    that's expected on the very first run.
    """
    try:
        return athena_read_sql(
            'SELECT surrogate_key, row_hash, version FROM "hashed_iceberg" WHERE is_current = true'
        )
    except Exception:
        logger.info("hashed_iceberg not queryable yet (first run) - treating current versions as empty")
        return pd.DataFrame(columns=["surrogate_key", "row_hash", "version"])


def expire_current_rows(surrogate_keys: list) -> None:
    """Close out the current version for each surrogate_key whose data changed."""
    if not surrogate_keys:
        return
    now = _now_ts()
    keys_sql = ",".join(f"'{k}'" for k in surrogate_keys)
    sql = f"""
    UPDATE {GLUE_DATABASE}.hashed_iceberg
    SET is_current = false, effective_end_date = TIMESTAMP '{now}'
    WHERE is_current = true AND surrogate_key IN ({keys_sql})
    """
    execute_athena_sql(sql, f"SCD2: expire {len(surrogate_keys)} changed row(s) in hashed_iceberg")


def apply_scd2(incoming_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare incoming rows against the current SCD2 state and return only the
    rows that need to be WRITTEN this run (new + changed). Unchanged rows are
    dropped here - writing them again would be a wasted, non-idempotent no-op.
    """
    incoming_df = incoming_df.copy()
    incoming_df["row_hash"] = compute_row_hash(incoming_df)

    current = read_current_versions()
    merged = incoming_df.merge(current, on="surrogate_key", how="left", suffixes=("", "_current"))

    is_new = merged["row_hash_current"].isna()
    is_changed = ~is_new & (merged["row_hash"] != merged["row_hash_current"])
    is_unchanged = ~is_new & ~is_changed

    logger.info(
        "SCD2 comparison: %d new, %d changed (new version), %d unchanged (skipped, no write)",
        int(is_new.sum()), int(is_changed.sum()), int(is_unchanged.sum()),
    )

    # Close out the current version for anything that changed, BEFORE inserting the new version
    expire_current_rows(merged.loc[is_changed, "surrogate_key"].tolist())

    to_write = merged[is_new | is_changed].copy()
    if to_write.empty:
        return to_write

    # .infer_objects(copy=False) between fillna() and astype(int): fillna()
    # on this object-dtype column (float for existing rows, NaN for brand
    # new ones) used to silently downcast to a numeric dtype on its own;
    # newer pandas warns that behaviour is going away and wants it done
    # explicitly - this is pandas' own suggested fix, not a functional
    # change (NaN (new) -> 0+1 = 1, same as before).
    to_write["version"] = to_write["version"].fillna(0).infer_objects(copy=False).astype(int) + 1

    # effective_start_date/effective_end_date must be typed as real pandas
    # datetime64 values, not plain strings/None - a column that's ALL None
    # (every row here has effective_end_date=NULL on write; it only gets
    # set later by expire_current_rows()'s UPDATE) comes out as pandas'
    # generic "object" dtype, which awswrangler can't map to an Iceberg
    # column type at all ("Impossible to infer the equivalent Athena data
    # type... too generic data type (object)") - hit on a real first run,
    # since CREATE TABLE has no existing schema to fall back on. Assigning
    # real pd.Timestamp/pd.NaT values instead makes pandas infer
    # datetime64[ns] for both columns, which awswrangler maps to Iceberg's
    # TIMESTAMP - matching what expire_current_rows()'s SQL already assumes.
    now_ts = pd.Timestamp.now(tz="UTC").tz_localize(None)
    to_write["effective_start_date"] = now_ts
    to_write["effective_end_date"] = pd.NaT
    to_write["is_current"] = True
    to_write["scd_key"] = to_write.apply(
        lambda r: hash_value(f"{r['surrogate_key']}#{r['version']}"), axis=1
    )
    return to_write.drop(columns=["row_hash_current"], errors="ignore")


def main() -> None:
    transformed_df = read_iceberg("transformed")
    logger.info("Read %d rows from transformed_iceberg", len(transformed_df))

    missing_identifier = transformed_df["resale_identifier"].isna() | (transformed_df["resale_identifier"] == "")
    hashable = transformed_df[~missing_identifier].copy()
    unhashable = transformed_df[missing_identifier]
    route_to_failed(unhashable, reason="missing_resale_identifier", stage="hashed")

    # Irreversible hash of the Resale Identifier itself (the original requirement)
    hashable["resale_identifier_hash"] = hashable["resale_identifier"].apply(hash_value)

    # SCD2 versioning on top of that (new requirement) - only new/changed rows get written
    scd2_batch = apply_scd2(hashable)
    write_iceberg(scd2_batch, stage="hashed", mode="append")  # plain append is correct here -
    # apply_scd2 already guarantees these are new version rows, never duplicates of an existing one

    logger.info(
        "job_5 complete: %d new/changed SCD2 version(s) written to hashed_iceberg, %d rejected (missing identifier)",
        len(scd2_batch), len(unhashable),
    )
    record_audit(
        job_name="job_5_hashed_iceberg", stage="hashed",
        rows_in=len(transformed_df), rows_out=len(scd2_batch), rows_rejected=len(unhashable),
    )


if __name__ == "__main__":
    main()
