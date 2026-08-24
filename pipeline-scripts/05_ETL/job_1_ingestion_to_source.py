"""
job_1_ingestion_to_source.py
=============================
Glue Python Shell job. First stage of the pipeline.

    data.gov.sg API -> S3 "source" (raw files, as-is)

Discovers all datasets in the "Resale Flat Prices" collection via the
Collection Metadata API (no hardcoded dataset_ids), filters to those
overlapping the required date range, then for each: initiate-download ->
poll-download -> stream the CSV straight into S3, byte-for-byte as
downloaded. No parsing, no cleaning - that happens in later stages.

IMPORTANT: verify the initiate/poll-download domain & response shape
against https://guide.data.gov.sg before a production run - docs showed
this on api-open.data.gov.sg/v1 vs the v2 api-production domain used for
collection metadata.
"""

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Tuple

import boto3
import requests

from common import get_logger, get_table_parameter, get_watermark, record_audit
from config import (
    COLLECTION_API_BASE,
    COLLECTION_ID,
    DATASET_API_BASE,
    DATE_RANGE_END,
    DATE_RANGE_START,
    LOOKBACK_WINDOW_DAYS,
    POLL_INTERVAL_SECONDS,
    POLL_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    SOURCE_S3_BUCKET,
    SOURCE_S3_PREFIX,
)

logger = get_logger("job_1_ingestion_to_source")
s3_client = boto3.client("s3")

# resaleflat_price's table_id in metadata_tables / table_parameters /
# table_watermarks - the same id every other job's write_by_load_type() /
# get_table_parameter() call already uses for this dataset (see
# 01_metadata_setup.py's seed data).
TARGET_TABLE_ID = 1


@dataclass
class DatasetRef:
    dataset_id: str
    name: str
    coverage_start: date
    coverage_end: date


def _parse_date(iso_ts: str) -> date:
    return datetime.fromisoformat(iso_ts).date()


def get_collection_datasets(collection_id: int) -> List[DatasetRef]:
    url = f"{COLLECTION_API_BASE}/collections/{collection_id}/metadata"
    resp = requests.get(url, params={"withDatasetMetadata": "true"}, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errorMsg"):
        raise RuntimeError(f"Collection metadata API error: {payload['errorMsg']}")

    metas = payload["data"].get("datasetMetadata", [])
    if not metas:
        raise RuntimeError("No datasetMetadata returned - did you forget withDatasetMetadata=true?")

    return [
        DatasetRef(
            dataset_id=m["datasetId"],
            name=m.get("name", m["datasetId"]),
            coverage_start=_parse_date(m["coverageStart"]),
            coverage_end=_parse_date(m["coverageEnd"]),
        )
        for m in metas
    ]


def filter_datasets_by_range(datasets: List[DatasetRef], start: date, end: date) -> List[DatasetRef]:
    matching = [d for d in datasets if d.coverage_start <= end and d.coverage_end >= start]
    logger.info("%d of %d datasets overlap %s to %s", len(matching), len(datasets), start, end)
    if not matching:
        raise RuntimeError("No datasets matched the required date range.")
    return matching


def initiate_download(dataset_id: str) -> None:
    url = f"{DATASET_API_BASE}/datasets/{dataset_id}/initiate-download"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errorMsg"):
        raise RuntimeError(f"initiate-download error for {dataset_id}: {payload['errorMsg']}")


def poll_download(dataset_id: str) -> str:
    url = f"{DATASET_API_BASE}/datasets/{dataset_id}/poll-download"
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errorMsg"):
            raise RuntimeError(f"poll-download error for {dataset_id}: {payload['errorMsg']}")
        download_url = payload.get("data", {}).get("url")
        if download_url:
            return download_url
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Timed out waiting for download URL for dataset_id={dataset_id}")


def download_to_source(dataset_id: str, download_url: str) -> str:
    s3_key = f"{SOURCE_S3_PREFIX}/dataset_id={dataset_id}/{dataset_id}.csv"
    with requests.get(download_url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS) as r:
        r.raise_for_status()
        s3_client.upload_fileobj(r.raw, SOURCE_S3_BUCKET, s3_key)
    logger.info("Landed source file: s3://%s/%s", SOURCE_S3_BUCKET, s3_key)
    return f"s3://{SOURCE_S3_BUCKET}/{s3_key}"


def resolve_effective_date_range() -> Tuple[date, date]:
    """
    Decide the actual [start, end] window to pull from data.gov.sg this run,
    driven by table_parameters.load_type - the SAME metadata switch
    write_by_load_type() (common.py) and the watermark-gating in
    context_tracking.py/orchestration.py already use, so "full vs
    incremental" stays a metadata decision everywhere in the pipeline, not
    something job_1 decides on its own.

    load_type='FULL' (this pipeline's default today - see common.py's
    overwrite_iceberg() docstring): always pull the complete configured
    range, DATE_RANGE_START..DATE_RANGE_END. Every downstream stage
    truncates and reloads from whatever job_1 lands, so pulling anything
    less would make the "full" load quietly incomplete.

    load_type='MERGE'/'INCREMENTAL'/'UPSERT': narrow the start of the range
    to (last watermark date - lookback_window days) - standard incremental-
    extraction pattern: only re-pull data at/after roughly where the last
    successful run left off, plus a buffer of N days BEFORE that to catch
    source records that arrived or were corrected late (see
    config.LOOKBACK_WINDOW_DAYS's docstring). Never narrower than
    DATE_RANGE_START. Falls back to the full configured range if no
    watermark has been set yet (the very first incremental run) - there's
    no "since" to narrow from until one successful run has landed.

    NOTE: data.gov.sg's Collection Metadata API only exposes each dataset's
    coverage_start/coverage_end at whole-dataset granularity (see
    get_collection_datasets()/filter_datasets_by_range()) - there's no
    "give me only rows changed since <date>" endpoint. So narrowing the
    range here means "pull fewer/only-recent whole datasets", not a
    row-level delta - the row-level idempotency (replace vs merge) still
    happens downstream, per-stage, via write_by_load_type().
    """
    full_start = date.fromisoformat(DATE_RANGE_START)
    full_end = date.fromisoformat(DATE_RANGE_END)

    load_type = get_table_parameter(TARGET_TABLE_ID, "load_type", default="FULL").strip().upper()
    if load_type == "FULL":
        logger.info("load_type=FULL -> pulling full configured range %s to %s", full_start, full_end)
        return full_start, full_end

    watermark_raw = get_watermark(TARGET_TABLE_ID, default=None)
    if not watermark_raw or watermark_raw.strip().startswith("1900-01-01"):
        # No real watermark yet (either no row at all, or still the
        # 1900-01-01 seed value 01_metadata_setup.py inserts before the
        # first successful run) - nothing to narrow from, so behave like FULL.
        logger.info(
            "load_type=%s but no watermark set yet - first incremental run, pulling full configured range %s to %s",
            load_type, full_start, full_end,
        )
        return full_start, full_end

    lookback_days = int(get_table_parameter(TARGET_TABLE_ID, "lookback_window", default=str(LOOKBACK_WINDOW_DAYS)))
    watermark_date = datetime.fromisoformat(watermark_raw.strip()).date()
    effective_start = max(full_start, watermark_date - timedelta(days=lookback_days))

    logger.info(
        "load_type=%s -> narrowing pull to %s to %s (watermark=%s, lookback_window=%dd)",
        load_type, effective_start, full_end, watermark_date, lookback_days,
    )
    return effective_start, full_end


def main() -> List[str]:
    datasets = get_collection_datasets(COLLECTION_ID)
    range_start, range_end = resolve_effective_date_range()
    matching = filter_datasets_by_range(datasets, range_start, range_end)

    source_paths = []
    for d in matching:
        initiate_download(d.dataset_id)
        url = poll_download(d.dataset_id)
        source_paths.append(download_to_source(d.dataset_id, url))

    logger.info("job_1 complete: %d source files landed", len(source_paths))
    record_audit(
        job_name="job_1_ingestion_to_source", stage="source",
        rows_in=len(matching), rows_out=len(source_paths), rows_rejected=len(matching) - len(source_paths),
    )
    return source_paths


if __name__ == "__main__":
    main()
