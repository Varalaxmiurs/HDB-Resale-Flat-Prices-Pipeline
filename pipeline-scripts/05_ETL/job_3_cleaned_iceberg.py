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
    7. Additional rules (all routed through route_to_failed() like every
       other rule above): floor_area_sqm within a realistic HDB range,
       lease_commence_date's year no later than the transaction's own year
       (a flat can't be sold before its lease exists), resale_price
       strictly positive, and up-front text normalization
       (upper-case + strip on town/street_name/flat_model) so formatting
       noise can't slip past either the categorical checks or the
       composite-key dedup below.

ASSUMPTIONS (document these in your final write-up too):
    - lease_commence_date is a year only; lease is assumed to start
      1 Jan of that year. Remaining lease is floored (never rounded up):
      today's own in-progress month counts as already elapsed - see
      recompute_remaining_lease()'s BUGFIX comment for why that matters.
    - "Statistical properties" for categorical validation = a value is
      valid if it occurs at least MIN_CATEGORY_FREQUENCY (5) times across
      the master dataset, rather than a hardcoded external whitelist. An
      earlier version of this rule built its "known good" set from the
      very column it was validating, which is circular and can never
      actually reject anything but a blank/null cell - see
      validate_categorical()'s BUGFIX comment.
    - Anomalous price = outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] within each
      (town, flat_type) group - a standard, explainable outlier heuristic.
"""

import re
from datetime import date

import pandas as pd

from common import get_logger, read_iceberg, record_audit, route_to_failed, write_by_load_type
from config import LEASE_YEARS, MIN_CATEGORY_FREQUENCY

logger = get_logger("job_3_cleaned_iceberg")

STOREY_RANGE_PATTERN = re.compile(r"^\d{2} TO \d{2}$")

# Realistic HDB floor-area sanity range (sqm) - deliberately generous (wide
# enough to include every real flat type, from 1-room to executive/multi-
# generation) so this only catches obvious data-entry errors (a misplaced
# decimal, a unit mix-up), not genuine variation within a flat type.
FLOOR_AREA_MIN_SQM = 20
FLOOR_AREA_MAX_SQM = 300

# Free-text categorical columns normalized (upper-case + strip) before any
# validation or duplicate-key comparison runs, so " Ang Mo Kio" / "ANG MO
# KIO" / "ang mo kio " aren't treated as 3 different categories
# (validate_categorical) or 3 different composite keys (resolve_duplicates)
# when they're really the same value with formatting noise.
NORMALIZE_TEXT_COLUMNS = ["town", "street_name", "flat_model"]


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Applied once, up front (before validation/dedup) - see
    NORMALIZE_TEXT_COLUMNS' docstring above for why."""
    df = df.copy()
    for col in NORMALIZE_TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    return df


# --------------------------------------------------------------------------- #
# Field validation rules
# --------------------------------------------------------------------------- #

def validate_date(df: pd.DataFrame) -> pd.Series:
    """`month` column expected as 'YYYY-MM' and parseable."""
    parsed = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    return parsed.notna()


def validate_categorical(df: pd.DataFrame, column: str) -> pd.Series:
    """
    Valid = value is non-null/non-blank AND occurs at least
    MIN_CATEGORY_FREQUENCY times across the master dataset - a value that
    appears only once or twice is statistically far more likely to be a
    typo or garbage than a genuinely rare-but-valid category, given how
    heavily each real town/flat_type/flat_model value repeats across
    thousands of transactions. See MIN_CATEGORY_FREQUENCY's module-level
    docstring for the full rationale (this replaced an earlier, circular
    version of this check - see the BUGFIX note above it).
    """
    values = df[column].astype(str).str.strip()
    non_blank = (values != "") & df[column].notna()
    frequency = values.map(values[non_blank].value_counts())
    return non_blank & (frequency >= MIN_CATEGORY_FREQUENCY)


def validate_storey_range(df: pd.DataFrame) -> pd.Series:
    return df["storey_range"].astype(str).str.strip().str.match(STOREY_RANGE_PATTERN)


def validate_floor_area_bounds(df: pd.DataFrame) -> pd.Series:
    """floor_area_sqm must fall within a realistic HDB range - see
    FLOOR_AREA_MIN_SQM/MAX_SQM's docstring above."""
    area = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    return area.notna() & (area >= FLOOR_AREA_MIN_SQM) & (area <= FLOOR_AREA_MAX_SQM)


def validate_lease_commence_vs_transaction(df: pd.DataFrame) -> pd.Series:
    """A flat cannot be sold before its own lease commences -
    lease_commence_date's year must be <= the transaction month's year."""
    def _valid(lease_year, month_str) -> bool:
        try:
            return int(lease_year) <= int(str(month_str).split("-")[0])
        except (TypeError, ValueError, IndexError):
            return False
    return pd.Series(
        [_valid(ly, m) for ly, m in zip(df["lease_commence_date"], df["month"])], index=df.index
    )


def validate_resale_price_positive(df: pd.DataFrame) -> pd.Series:
    """resale_price must be a real, positive number - checked explicitly,
    up front, before flag_anomalous_price()'s IQR check runs (a zero/
    negative price would otherwise just get silently averaged into that
    group's quartiles rather than being rejected outright)."""
    price = pd.to_numeric(df["resale_price"], errors="coerce")
    return price.notna() & (price > 0)


def run_field_validations(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a `_validation_errors` column: list of failed rule names per row."""
    checks = {
        "invalid_date": ~validate_date(df),
        "invalid_town": ~validate_categorical(df, "town"),
        "invalid_flat_type": ~validate_categorical(df, "flat_type"),
        "invalid_flat_model": ~validate_categorical(df, "flat_model"),
        "invalid_storey_range": ~validate_storey_range(df),
        "invalid_floor_area": ~validate_floor_area_bounds(df),
        "lease_commence_after_transaction": ~validate_lease_commence_vs_transaction(df),
        "invalid_resale_price": ~validate_resale_price_positive(df),
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

        # BUGFIX (2026-08-25): "rounded down" means remaining lease should be
        # TRUNCATED (never overstated) - any partial month already in
        # progress today counts as already elapsed, not as still remaining.
        # The previous formula used (today.month - 1), which leaves today's
        # own partial month out of "elapsed" entirely - every day past the
        # 1st of the month, that quietly credited one extra month onto
        # "remaining" (rounding UP, the opposite of what the brief asks
        # for). Adding 1 once we're past day 1 makes the current in-progress
        # month count as elapsed too, so remaining is always floored.
        elapsed_months = (today.year - commence_year) * 12 + (today.month - 1)  # assume Jan commencement
        if today.day > 1:
            elapsed_months += 1
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

    raw_df = normalize_text_columns(raw_df)  # before any validation/dedup - see its docstring

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
