"""
job_4_transformed_iceberg.py
==============================
Glue Python Shell (or Spark) job. Fourth stage of the pipeline.

    cleaned_iceberg -> transformed_iceberg (+ failed_iceberg for rejects)

Implements the Data Transformation Requirements from the test brief:
    1. Build the "Resale Identifier" column:
         - "S"
         - + 3 digits: first 3 digits of `block` (non-digit chars stripped),
           zero-padded on the left if fewer than 3 digits remain
         - + 2 digits: first 2 digits of the average resale_price for that
           row's (year-month, town, flat_type) group
         - + 2 digits: the month (MM) of that row's transaction month
         - + 1 char: first character of `town`
    2. Resolve any remaining duplicates (keep higher price).
    3. Hashing itself happens in job_5, not here.
"""

import pandas as pd

from common import get_logger, merge_iceberg, read_iceberg, record_audit, route_to_failed

logger = get_logger("job_4_transformed_iceberg")


def _block_digits(block) -> str:
    digits = "".join(ch for ch in str(block) if ch.isdigit())
    return digits[:3].zfill(3) if digits else None


def _avg_price_digits(avg_price: float) -> str:
    if pd.isna(avg_price):
        return None
    int_part = str(int(avg_price))
    return int_part[:2].zfill(2)


def _month_digits(month_str: str) -> str:
    try:
        return str(month_str).split("-")[1].zfill(2)
    except (IndexError, AttributeError):
        return None


def _town_char(town) -> str:
    town = str(town).strip()
    return town[0].upper() if town else None


def build_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Average resale price grouped by year-month, town, flat_type
    group_avg = df.groupby(["month", "town", "flat_type"])["resale_price"].transform("mean")

    block_part = df["block"].apply(_block_digits)
    price_part = group_avg.apply(_avg_price_digits)
    month_part = df["month"].apply(_month_digits)
    town_part = df["town"].apply(_town_char)

    identifier = (
        "S"
        + block_part.fillna("")
        + price_part.fillna("")
        + month_part.fillna("")
        + town_part.fillna("")
    )
    # A row is only valid if every component function actually produced a
    # value AND the assembled identifier is the expected full length:
    # "S" (1) + block (3) + avg-price (2) + month (2) + town (1) = 9 chars.
    # Checking length alone previously used 8, which is wrong (1+3+2+2+1=9)
    # and rejected every well-formed identifier - checking component
    # completeness directly is a more robust signal than length alone.
    has_all_components = (
        block_part.notna() & price_part.notna() & month_part.notna() & town_part.notna()
    )
    df["resale_identifier"] = identifier
    df["_identifier_valid"] = has_all_components & (identifier.str.len() == 9)
    return df


def resolve_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    key_cols = [c for c in df.columns if c not in ("resale_price", "surrogate_key") and not c.startswith("_")]
    df = df.sort_values("resale_price", ascending=False)
    is_dup = df.duplicated(subset=key_cols, keep="first")
    return df[~is_dup], df[is_dup]


def main() -> None:
    cleaned_df = read_iceberg("cleaned")
    logger.info("Read %d rows from cleaned_iceberg", len(cleaned_df))

    with_identifier = build_resale_identifier(cleaned_df)

    valid = with_identifier[with_identifier["_identifier_valid"]].drop(columns=["_identifier_valid"])
    invalid = with_identifier[~with_identifier["_identifier_valid"]]
    route_to_failed(invalid, reason="incomplete_resale_identifier", stage="transformed")

    kept, discarded_dupes = resolve_duplicates(valid)
    route_to_failed(discarded_dupes, reason="duplicate_key_lower_price", stage="transformed")

    merge_iceberg(kept, stage="transformed")  # idempotent upsert on surrogate_key (carried from cleaned)
    logger.info(
        "job_4 complete: %d passed to transformed_iceberg, %d identifier-rejects, %d dupes",
        len(kept), len(invalid), len(discarded_dupes),
    )
    record_audit(
        job_name="job_4_transformed_iceberg", stage="transformed",
        rows_in=len(cleaned_df), rows_out=len(kept), rows_rejected=len(invalid) + len(discarded_dupes),
    )


if __name__ == "__main__":
    main()
