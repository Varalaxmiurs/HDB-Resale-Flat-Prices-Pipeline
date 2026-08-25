"""
job_2b_data_profiling.py
==========================
Runs after job_2 (raw_iceberg) and before job_3 (cleaned_iceberg).

    raw_iceberg -> profiling report (S3, not another Iceberg stage)

Satisfies Data Quality Requirement #2 ("Perform Data Profiling on the
dataset") and, just as importantly, produces the documented statistical
basis for Requirement #3's field validation rules in job_3 - so a reviewer
can see *why* job_3's category-membership / date-format / storey-range
checks are the right checks for this dataset, rather than taking them on
faith.

This is deliberately dependency-light (pandas only) rather than pulling in
a framework like ydata-profiling/Great Expectations, so it runs anywhere
job_2 runs without extra packages on the Glue Python Shell image. Swap in
one of those frameworks here if a richer HTML report is wanted - the
profile_dataframe() function is the single place that would change.

Output: one JSON report + one human-readable Markdown summary, written to
S3 alongside the other stage outputs (not into an Iceberg table, since a
profile is a point-in-time report about a run, not a row-level dataset).
"""

import json
from datetime import datetime, timezone

import pandas as pd

from common import get_logger, read_iceberg, record_audit
from config import AUDIT_S3_BUCKET, MIN_CATEGORY_FREQUENCY

logger = get_logger("job_2b_data_profiling")

REPORT_S3_PREFIX = f"s3://{AUDIT_S3_BUCKET}/profiling_reports"


# --------------------------------------------------------------------------- #
# Profiling
# --------------------------------------------------------------------------- #

def _column_profile(series: pd.Series) -> dict:
    non_null = series.dropna()
    profile = {
        "dtype": str(series.dtype),
        "row_count": int(len(series)),
        "null_count": int(series.isna().sum()),
        "null_pct": round(float(series.isna().mean()) * 100, 2),
        "distinct_count": int(non_null.nunique()),
    }

    if pd.api.types.is_numeric_dtype(series):
        profile.update({
            "min": float(non_null.min()) if not non_null.empty else None,
            "max": float(non_null.max()) if not non_null.empty else None,
            "mean": float(non_null.mean()) if not non_null.empty else None,
            "std": float(non_null.std()) if not non_null.empty else None,
            "q1": float(non_null.quantile(0.25)) if not non_null.empty else None,
            "median": float(non_null.quantile(0.5)) if not non_null.empty else None,
            "q3": float(non_null.quantile(0.75)) if not non_null.empty else None,
        })
    else:
        # Value distribution is exactly what job_3's validate_categorical()
        # relies on: "valid" = a value that actually occurs in the data.
        top_values = non_null.astype(str).value_counts().head(20)
        profile["top_values"] = top_values.to_dict()
        profile["distinct_values_sample"] = sorted(non_null.astype(str).unique().tolist())[:50]

    return profile


def _storey_range_profile(df: pd.DataFrame) -> dict:
    """Parses storey_range ('01 TO 03') into low/high ints and reports the
    observed range plus any structurally-anomalous rows (low > high, or
    both zero e.g. '00 TO 00') - visibility ahead of job_3's format-only
    regex check, which only verifies the SHAPE of storey_range, not
    whether the two numbers actually make sense together."""
    if "storey_range" not in df.columns:
        return {}
    parts = df["storey_range"].astype(str).str.strip().str.extract(r"^(\d{2}) TO (\d{2})$")
    low = pd.to_numeric(parts[0], errors="coerce")
    high = pd.to_numeric(parts[1], errors="coerce")
    valid_pair = low.notna() & high.notna()
    anomalous = valid_pair & ((low > high) | ((low == 0) & (high == 0)))
    return {
        "min_storey": float(low[valid_pair].min()) if valid_pair.any() else None,
        "max_storey": float(high[valid_pair].max()) if valid_pair.any() else None,
        "unparseable_rows": int((~valid_pair).sum()),
        "anomalous_range_rows": int(anomalous.sum()),
    }


def _block_format_profile(df: pd.DataFrame) -> dict:
    """Buckets block values by shape: pure numeric ('108'), alphanumeric
    ('406A'), or 'other' (anything else) - visibility into how consistent
    block formatting actually is, ahead of job_4's _block_digits()
    extraction."""
    if "block" not in df.columns:
        return {}
    values = df["block"].astype(str).str.strip()
    is_numeric = values.str.match(r"^\d+$")
    is_alnum = values.str.match(r"^\d+[A-Za-z]+$")
    other = ~(is_numeric | is_alnum)
    return {
        "numeric_count": int(is_numeric.sum()),
        "alphanumeric_count": int(is_alnum.sum()),
        "other_format_count": int(other.sum()),
        "other_format_sample": sorted(values[other].unique().tolist())[:20],
    }


def _rare_category_profile(df: pd.DataFrame, columns: list) -> dict:
    """Values occurring fewer than MIN_CATEGORY_FREQUENCY times in a
    categorical column - could be a genuinely rare-but-real category
    (e.g. an uncommon flat_model) or a transcription typo. Purely
    informational here - job_3's validate_categorical() is what actually
    rejects on this, using the SAME threshold (config.MIN_CATEGORY_FREQUENCY),
    so this just makes "what's rare and why" visible to a reviewer ahead of
    that rejection happening."""
    rare = {}
    for col in columns:
        if col not in df.columns:
            continue
        values = df[col].astype(str).str.strip()
        counts = values[values != ""].value_counts()
        rare[col] = counts[counts < MIN_CATEGORY_FREQUENCY].to_dict()
    return rare


def profile_dataframe(df: pd.DataFrame, dataset_name: str) -> dict:
    """Produce a profiling report: shape, per-column stats, and duplicate-key
    counts, all derived from the data itself (no external whitelists)."""
    report = {
        "dataset_name": dataset_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": {col: _column_profile(df[col]) for col in df.columns},
    }

    if {"resale_price"}.issubset(df.columns):
        key_cols = [c for c in df.columns if c not in ("resale_price", "surrogate_key")]
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        report["composite_key_duplicate_rows"] = int(dup_mask.sum())

    report["storey_range_profile"] = _storey_range_profile(df)
    report["block_format_profile"] = _block_format_profile(df)
    report["rare_categories"] = _rare_category_profile(df, ["town", "flat_type", "flat_model"])

    return report


def render_markdown_summary(report: dict) -> str:
    lines = [
        f"# Data Profiling Report - {report['dataset_name']}",
        f"Generated: {report['generated_at']}",
        "",
        f"- Rows: {report['row_count']}",
        f"- Columns: {report['column_count']}",
    ]
    if "composite_key_duplicate_rows" in report:
        lines.append(f"- Rows sharing a composite key (differ only by resale_price): "
                      f"{report['composite_key_duplicate_rows']}")
    sp = report.get("storey_range_profile") or {}
    if sp:
        lines.append(
            f"- Storey range: min={sp.get('min_storey')}, max={sp.get('max_storey')}, "
            f"unparseable={sp.get('unparseable_rows')}, "
            f"anomalous (low>high or 00 TO 00)={sp.get('anomalous_range_rows')}"
        )
    bp = report.get("block_format_profile") or {}
    if bp:
        lines.append(
            f"- Block formats: numeric={bp.get('numeric_count')}, "
            f"alphanumeric={bp.get('alphanumeric_count')}, other={bp.get('other_format_count')} "
            f"(sample: {bp.get('other_format_sample')})"
        )
    rare = report.get("rare_categories") or {}
    if any(rare.values()):
        lines.append("- Rare categories (< MIN_CATEGORY_FREQUENCY occurrences - possible typos):")
        for col, vals in rare.items():
            if vals:
                lines.append(f"  - {col}: {vals}")
    lines.append("")
    lines.append("## Per-column summary")
    for col, prof in report["columns"].items():
        lines.append(f"\n### `{col}` ({prof['dtype']})")
        lines.append(f"- nulls: {prof['null_count']} ({prof['null_pct']}%) | distinct: {prof['distinct_count']}")
        if "mean" in prof:
            lines.append(
                f"- min={prof['min']}, q1={prof['q1']}, median={prof['median']}, "
                f"q3={prof['q3']}, max={prof['max']}, mean={round(prof['mean'], 2) if prof['mean'] is not None else None}, "
                f"std={round(prof['std'], 2) if prof['std'] is not None else None}"
            )
        else:
            top = list(prof.get("top_values", {}).items())[:5]
            lines.append(f"- top values: {top}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    raw_df = read_iceberg("raw")
    logger.info("Read %d rows from raw_iceberg for profiling", len(raw_df))

    report = profile_dataframe(raw_df, dataset_name="raw_iceberg")
    markdown = render_markdown_summary(report)

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = f"{REPORT_S3_PREFIX}/raw_iceberg_{run_stamp}.json"
    md_path = f"{REPORT_S3_PREFIX}/raw_iceberg_{run_stamp}.md"

    _write_text_to_s3(json.dumps(report, indent=2, default=str), json_path)
    _write_text_to_s3(markdown, md_path)

    logger.info("job_2b complete: profiling report written to %s and %s", json_path, md_path)
    record_audit(
        job_name="job_2b_data_profiling", stage="raw",
        rows_in=len(raw_df), rows_out=len(raw_df), rows_rejected=0,
    )


def _write_text_to_s3(text: str, s3_path: str) -> None:
    import boto3
    from urllib.parse import urlparse

    parsed = urlparse(s3_path)
    boto3.client("s3").put_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Body=text.encode("utf-8"))


if __name__ == "__main__":
    main()
