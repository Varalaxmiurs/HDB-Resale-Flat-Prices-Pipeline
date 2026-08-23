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

RATE LIMITING: data.gov.sg's API returns 429 (Too Many Requests) if you hit
initiate-download/poll-download back-to-back across datasets with no gap -
this was observed in practice on the 2nd dataset of a 3-dataset run. Two
mitigations below:
    - _get_with_retry() retries any 429/5xx with exponential backoff
      (honoring a Retry-After header if the API sends one).
    - main() sleeps INTER_DATASET_DELAY_SECONDS between datasets so we
      don't lean on retries alone to stay under the rate limit.
"""

import random
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import List

import boto3
import requests

from common import get_logger, record_audit
from config import (
    COLLECTION_API_BASE,
    COLLECTION_ID,
    DATASET_API_BASE,
    DATE_RANGE_END,
    DATE_RANGE_START,
    POLL_INTERVAL_SECONDS,
    POLL_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    SOURCE_S3_BUCKET,
    SOURCE_S3_PREFIX,
)

logger = get_logger("job_1_ingestion_to_source")
s3_client = boto3.client("s3")

MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
INTER_DATASET_DELAY_SECONDS = 2  # gap between datasets, to avoid tripping the rate limit at all


@dataclass
class DatasetRef:
    dataset_id: str
    name: str
    coverage_start: date
    coverage_end: date


def _parse_date(iso_ts: str) -> date:
    return datetime.fromisoformat(iso_ts).date()


def _get_with_retry(url: str, **kwargs) -> requests.Response:
    """GET with exponential backoff on 429 (rate limit) and 5xx responses.

    Honors a Retry-After header when the API sends one; otherwise backs off
    2s, 4s, 8s, 16s (+ up to 1s jitter) across MAX_RETRIES attempts. Raises
    via raise_for_status() if still failing on the last attempt.
    """
    backoff = INITIAL_BACKOFF_SECONDS
    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, **kwargs)
        if resp.status_code != 429 and resp.status_code < 500:
            break
        if attempt == MAX_RETRIES:
            break
        retry_after = resp.headers.get("Retry-After")
        wait = float(retry_after) if retry_after else backoff + random.uniform(0, 1)
        logger.warning(
            "GET %s -> %d, retrying in %.1fs (attempt %d/%d)",
            url, resp.status_code, wait, attempt, MAX_RETRIES,
        )
        time.sleep(wait)
        backoff *= 2
    resp.raise_for_status()
    return resp


def get_collection_datasets(collection_id: int) -> List[DatasetRef]:
    url = f"{COLLECTION_API_BASE}/collections/{collection_id}/metadata"
    resp = _get_with_retry(url, params={"withDatasetMetadata": "true"}, timeout=REQUEST_TIMEOUT_SECONDS)
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
    resp = _get_with_retry(url, timeout=REQUEST_TIMEOUT_SECONDS)
    payload = resp.json()
    if payload.get("errorMsg"):
        raise RuntimeError(f"initiate-download error for {dataset_id}: {payload['errorMsg']}")


def poll_download(dataset_id: str) -> str:
    url = f"{DATASET_API_BASE}/datasets/{dataset_id}/poll-download"
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        resp = _get_with_retry(url, timeout=REQUEST_TIMEOUT_SECONDS)
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


def main() -> List[str]:
    datasets = get_collection_datasets(COLLECTION_ID)
    range_start = date.fromisoformat(DATE_RANGE_START)
    range_end = date.fromisoformat(DATE_RANGE_END)
    matching = filter_datasets_by_range(datasets, range_start, range_end)

    source_paths = []
    for i, d in enumerate(matching):
        if i > 0:
            time.sleep(INTER_DATASET_DELAY_SECONDS)  # stay under the rate limit rather than just retrying into it
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
