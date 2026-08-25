"""
run_pipeline.py
================
Thin CLI wrapper around pipeline-scripts/05_ETL/orchestration.py's
run_pipeline() - same function pipeline_orchestration.ipynb calls, so the
actual run logic lives in exactly one place, not duplicated between the
notebook and this script.

Usage (from the hdb_1/ project root):

    cd ~/OneDrive/Desktop/claude/hdb_1
    python run_pipeline.py

    HDB_SKIP_INGESTION=1 python run_pipeline.py   # source files already exist

    # DEBUG: run just ONE stage, skip the rest of the chain entirely.
    # Handy while iterating on a single job - e.g. you just fixed job_3 and
    # want to rerun cleaned_iceberg against the raw_iceberg that's already
    # there, without re-running ingestion/raw_iceberg first.
    HDB_ONLY_STEP=cleaned_iceberg python run_pipeline.py
    HDB_ONLY_STEP=transformed_iceberg python run_pipeline.py
    HDB_ONLY_STEP=hashed_iceberg python run_pipeline.py
    # (valid values: ingestion_to_source, raw_iceberg, data_profiling,
    #  cleaned_iceberg, transformed_iceberg, hashed_iceberg)

Only supports local mode (calls each job's main() directly, in this
script's own process). Glue mode isn't wired up here since the 6 AWS Glue
Job resources it needs don't exist yet - see orchestration.py's docstring.
"""

import os
import sys
from pathlib import Path

# This script sits at the project root, while config.py / common.py /
# orchestration.py / job_*.py live in pipeline-scripts/05_ETL/ - add that
# folder to sys.path so they can be imported directly, same as the notebook.
sys.path.insert(0, str(Path(__file__).resolve().parent / "pipeline-scripts" / "05_ETL"))

from orchestration import run_pipeline

SKIP_INGESTION = os.environ.get("HDB_SKIP_INGESTION", "0") == "1"
ONLY_STEP = os.environ.get("HDB_ONLY_STEP") or None  # "" or unset -> None -> normal full-chain run

if __name__ == "__main__":
    pipeline_succeeded, run_log = run_pipeline(run_mode="local", skip_ingestion=SKIP_INGESTION, only_step=ONLY_STEP)
    if not pipeline_succeeded:
        sys.exit(1)
