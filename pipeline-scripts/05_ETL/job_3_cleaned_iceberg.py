"""
job_3_cleaned_iceberg.py
==========================
Glue Python Shell (or Spark) job. Third stage of the pipeline.

    raw_iceberg -> cleaned_iceberg (+ failed_iceberg for rejects)

Implements the Data Quality Requirements from the test brief:
    3. Field validation (Date, Town, Flat Type, Flat Model, storey_range)
       derived from the statistical properties of the dataset itself
       (no hardcoded whitelists).
    4. Recompute remaining lease as of today (99-year lease assumption),
       rounded down to years + months.
    5. Composite-key duplicate handling: same key, different resale_price
       -> keep the higher price, route the lower one to failed_iceberg.
    6. Anomalous resale price detection via IQR, grouped by town + flat_type.
    7. Any additional rule you add should also route rejects through
       route_to_failed() with a clear reason string.

ASSUMPTIONS (document these in your final write-up too):
    - lease_commence_date is a year only; lease is assumed to start
      1 Jan of that year.
    - "Statistical properties" for categorical validation = build the set
      of valid values from the dataset's own value_counts(), rather than
      a hardcoded external list, and flag rows whose value never appears
      (rather than a fixed frequency threshold, to avoid over-flagging a
      genuinely clean dataset as the brief notes may be the case).
    - Anomalous price = outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] within each
      (town, flat_type) group - a standard, explainable outlier heuristic.
"""

import re
from datetime import date

import pandas as pd

from common import get_logger, read_iceberg, record_audit, route_to_failed, write_by_load_type
from config import LEASE_YEARS

logger = get_logger("job_3_cleaned_iceberg")

STOREY_RANGE_PATTERN = re.compile(r"^\d{2} TO \d{2}$")


# --------------------------------------------------------------------------- #
# Field validation rules
# --------------------------------------------------------------------------- #

def validate_date(df: pd.DataFrame) -> pd.Series:
    """`month` column expected as 'YYYY-MM' and parseable."""
    parsed = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    return parsed.notna()


def validate_categorical(df: pd.DataFrame, column: str) -> pd.Series:
    """
    Valid = value is one of the values that actually occur in the dataset's
    own distribution (i.e. non-null, non-blank). This catches nulls/blank
    strings/obvious garbage without hardcoding an external whitelist of
    towns/flat types/models, per the "statistical properties" requirement.
    """
    values = df[column].astype(str).str.strip()
    known_values = set(values[values != ""].unique())
    return values.isin(known_values) & (values != "") & df[column].notna()


def validate_storey_range(df: pd.DataFrame) -> pd.Series:
    return df["storey_range"].astype(str).str.strip().str.match(STOREY_RANGE_PATTERN)


def run_field_validations(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a `_validation_errors` column: list of failed rule names per row."""
    checks = {
        "invalid_date": ~validate_date(df),
        "invalid_town": ~validate_categorical(df, "town"),
        "invalid_flat_type": ~validate_categorical(df, "flat_type"),
        "invalid_flat_model": ~validate_categorical(df, "flat_model"),
        "invalid_storey_range": ~validate_storey_range(df),
    }
    errors = pd.Series([[] for _ in range(len(df))], index=df.index)
    for rule_name, failed_mask in checks.items():
        errors.loc[failed_mask] = errors.loc[failed_mask].apply(lambda lst, r=rule_name: lst + [r])
    df = df.copy()
    df["_validation_errors"] = errors
    return df


# --------------------------------------------------------------------------- #
# Remaining lease recomputation
# --------------------------------------------------------------------------- #

def recompute_remaining_lease(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    today = date.today()

    def _remaining(lease_commence_year) -> pd.Series:
        try:
            commence_year = int(lease_commence_year)
        except (TypeError, ValueError):
            return pd.Series({"remaining_lease_years": None, "remaining_lease_months": None})

        elapsed_months = (today.year - commence_year) * 12 + (today.month - 1)  # assume Jan commencement
        remaining_months_total = LEASE_YEARS * 12 - elapsed_months
        remaining_months_total = max(remaining_months_total, 0)
        return pd.Series({
            "remaining_lease_years": remaining_months_total // 12,
            "remaining_lease_months": remaining_months_total % 12,
        })

    lease_cols = df["lease_commence_date"].apply(_remaining)
    df["remaining_lease_years"] = lease_cols["remaining_lease_years"]
    df["remaining_lease_months"] = lease_cols["remaining_lease_months"]
    return df


# --------------------------------------------------------------------------- #
# Duplicate composite-key handling
# --------------------------------------------------------------------------- #

def resolve_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Composite key = all columns except resale_price. Keep the higher price."""
    key_cols = [c for c in df.columns if c not in ("resale_price", "surrogate_key") and not c.startswith("_")]
    df = df.sort_values("resale_price", ascending=False)
    is_dup = df.duplicated(subset=key_cols, keep="first")
    kept, discarded = df[~is_dup], df[is_dup]
    logger.info("Duplicate-key resolution: kept %d, discarded %d (lower price)", len(kept), len(discarded))
    return kept, discarded


# --------------------------------------------------------------------------- #
# Anomalous price detection (IQR, grouped by town + flat_type)
# --------------------------------------------------------------------------- #

def flag_anomalous_price(df: pd.DataFrame) -> pd.Series:
    def _iqr_outlier_mask(group: pd.Series) -> pd.Series:
        q1, q3 = group.quantile(0.25), group.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        return (group < lower) | (group > upper)

    return df.groupby(["town", "flat_type"])["resale_price"].transform(_iqr_outlier_mask)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    raw_df = read_iceberg("raw")
    logger.info("Read %d rows from raw_iceberg", len(raw_df))

    validated = run_field_validations(raw_df)
    field_failed_mask = validated["_validation_errors"].apply(len).gt(0)
    field_passed = validated[~field_failed_mask].drop(columns=["_validation_errors"])
    field_failed = validated[field_failed_mask]

    route_to_failed(field_failed, reason="field_validation_failed", stage="cleaned")

    kept, discarded_dupes = resolve_duplicates(field_passed)
    route_to_failed(discarded_dupes, reason="duplicate_key_lower_price", stage="cleaned")

    kept = recompute_remaining_lease(kept)

    anomaly_mask = flag_anomalous_price(kept)
    clean_final = kept[~anomaly_mask]
    anomalous = kept[anomaly_mask]
    route_to_failed(anomalous, reason="anomalous_price_iqr_outlier", stage="cleaned")

    write_by_load_type(clean_final, stage="cleaned", table_id=1)  # FULL/MERGE decided by table_parameters
    total_rejected = len(field_failed) + len(discarded_dupes) + len(anomalous)
    logger.info(
        "job_3 complete: %d passed to cleaned_iceberg, %d field-rejects, %d dupes, %d anomalies",
        len(clean_final), len(field_failed), len(discarded_dupes), len(anomalous),
    )
    record_audit(
        job_name="job_3_cleaned_iceberg", stage="cleaned",
        rows_in=len(raw_df), rows_out=len(clean_final), rows_rejected=total_rejected,
    )


if __name__ == "__main__":
    main()
