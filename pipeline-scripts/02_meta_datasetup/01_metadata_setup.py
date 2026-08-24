import sys
import boto3
import time
import logging
import argparse
from pathlib import Path

# pipeline-scripts/05_ETL/config.py is the pipeline's single source of truth
# for bucket names, prefixes, and the natural key columns (see config.py's
# own docstring) - importing it here means the metadata seeded below is
# DERIVED from what the pipeline actually does, not a hand-typed copy that
# can silently drift out of sync with it (which is exactly what happened:
# source_path pointed at a path nothing ever wrote to, and primary_key was
# missing 3 of NATURAL_KEY_COLUMNS' 9 real columns).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "05_ETL"))
from config import LOOKBACK_WINDOW_DAYS, NATURAL_KEY_COLUMNS, SOURCE_S3_BUCKET, SOURCE_S3_PREFIX


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.WARNING,  # only show warnings/errors - silence per-query "started"/"succeeded" noise
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser(
    description="HDB Housing Metadata Iceberg Setup"
)

parser.add_argument(
    "--database",
    required=True,
    help="Glue/Athena database name"
)

parser.add_argument(
    "--workgroup",
    required=True,
    help="Athena workgroup"
)

parser.add_argument(
    "--metadata-bucket",
    required=True,
    help="S3 metadata bucket"
)

parser.add_argument(
    "--region",
    required=True,
    help="AWS region"
)

args = parser.parse_args()


DATABASE = args.database
WORKGROUP = args.workgroup
METADATA_BUCKET = args.metadata_bucket
REGION = args.region


# ============================================================
# AWS CLIENT
# ============================================================

athena = boto3.client(
    "athena",
    region_name=REGION
)

glue = boto3.client(
    "glue",
    region_name=REGION
)


def table_exists(table_name: str) -> bool:
    """Check the Glue Catalog directly rather than relying on Athena's own
    CREATE TABLE IF NOT EXISTS - that re-validates the existing table's
    Iceberg metadata even when it's a no-op, and that validation step is
    what's been intermittently failing with "Iceberg cannot find the
    requested entity" on tables that already exist and are fine. Checking
    via the Glue API first lets us skip the CREATE TABLE call entirely when
    it isn't needed, sidestepping that flaky path."""
    try:
        glue.get_table(DatabaseName=DATABASE, Name=table_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def database_exists() -> bool:
    """Same reasoning as table_exists() - CREATE SCHEMA IF NOT EXISTS hits
    the same flaky re-validation when the schema already exists, so check
    via the Glue API first and skip the query entirely when it's not needed."""
    try:
        glue.get_database(Name=DATABASE)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


# ============================================================
# METADATA LOCATION
# ============================================================

METADATA_LOCATION = (
    f"s3://{METADATA_BUCKET}/metadata_iceberg_table"
)


# ============================================================
# RUN ATHENA QUERY
# ============================================================

def run_query(sql, description):

    logger.info(
        f"Running query: {description}"
    )

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": DATABASE
        },
        ResultConfiguration={
            "OutputLocation": f"s3://{METADATA_BUCKET}/athena-results/"
        },
        WorkGroup=WORKGROUP
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info(
        f"Query started: {query_execution_id}"
    )

    return query_execution_id


# ============================================================
# WAIT FOR ATHENA QUERY
# ============================================================

def wait_for_query(query_execution_id):

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = (
            response["QueryExecution"]["Status"]["State"]
        )

        if status == "SUCCEEDED":

            logger.info(
                f"Query succeeded: {query_execution_id}"
            )

            return True

        elif status in ["FAILED", "CANCELLED"]:

            reason = (
                response["QueryExecution"]["Status"]
                .get(
                    "StateChangeReason",
                    "Unknown error"
                )
            )

            logger.error(
                f"Query failed: {reason}"
            )

            raise Exception(reason)

        time.sleep(2)


# ============================================================
# EXECUTE QUERY
# ============================================================

def execute_query(sql, description):

    query_id = run_query(
        sql,
        description
    )

    wait_for_query(query_id)

    # Brief pause between statements - firing Iceberg CREATE TABLE calls back
    # to back (observed ~2s apart) has intermittently failed with "Iceberg
    # cannot find the requested entity" on the very next statement, even
    # though each one is structurally identical to the one before it that
    # just succeeded. Looks like AWS-side propagation lag rather than a bug
    # in the SQL itself, so give it a few seconds of breathing room.
    time.sleep(5)


# ============================================================
# METADATA SETUP
# ============================================================

def setup_metadata():

    logger.info(
        "============================================================"
    )

    logger.info(
        "HDB Housing Metadata Iceberg Setup Started"
    )

    logger.info(
        f"Database         : {DATABASE}"
    )

    logger.info(
        f"Athena Workgroup : {WORKGROUP}"
    )

    logger.info(
        f"Metadata Bucket  : {METADATA_BUCKET}"
    )

    logger.info(
        f"Metadata Location: {METADATA_LOCATION}"
    )

    logger.info(
        "============================================================"
    )


    # ========================================================
    # 1. CREATE DATABASE
    # ========================================================

    create_database_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {DATABASE}
    """

    if database_exists():
        logger.info(f"Database already exists, skipping: {DATABASE}")
    else:
        execute_query(
            create_database_sql,
            "Create HDB database"
        )



    # ========================================================
    # 2. CREATE metadata_tables
    # ========================================================

    metadata_tables_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.metadata_tables (

        table_id        BIGINT,
        table_name      STRING,
        source_system   STRING,
        source_schema   STRING,
        source_table    STRING,
        source_path     STRING,
        target_layer    STRING,
        bronze_schema   STRING,
        silver_schema   STRING,
        gold_schema     STRING,
        active_flag     BOOLEAN,
        load_order      BIGINT,
        created_at      TIMESTAMP

    )
    LOCATION '{METADATA_LOCATION}/metadata_tables/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    metadata_tables_existed = table_exists("metadata_tables")
    if metadata_tables_existed:
        logger.info("Table already exists, skipping: metadata_tables")
    else:
        execute_query(
            metadata_tables_sql,
            "Create metadata_tables"
        )


    # # ========================================================
    # # 3. CREATE table_parameters
    # # ========================================================

    table_parameters_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.table_parameters (

        table_id        BIGINT,
        parameter_name  STRING,
        parameter_value STRING,
        created_at      TIMESTAMP

    )
    LOCATION '{METADATA_LOCATION}/table_parameters/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    table_parameters_existed = table_exists("table_parameters")
    if table_parameters_existed:
        logger.info("Table already exists, skipping: table_parameters")
    else:
        execute_query(
            table_parameters_sql,
            "Create table_parameters"
        )


    # ========================================================
    # 4. CREATE table_watermarks
    # ========================================================

    table_watermarks_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.table_watermarks (

        table_id             BIGINT,
        last_watermark_value STRING,
        last_updated_at      TIMESTAMP,
        last_run_id          BIGINT

    )
    PARTITIONED BY (table_id)
    LOCATION '{METADATA_LOCATION}/table_watermarks/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    table_watermarks_existed = table_exists("table_watermarks")
    if table_watermarks_existed:
        logger.info("Table already exists, skipping: table_watermarks")
    else:
        execute_query(
            table_watermarks_sql,
            "Create table_watermarks"
        )


    # ========================================================
    # 5. CREATE pipeline_runs
    # ========================================================

    pipeline_runs_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_runs (

        run_id              BIGINT,
        table_id            BIGINT,
        layer               STRING,
        start_time          TIMESTAMP,
        end_time            TIMESTAMP,
        status              STRING,
        number_of_records   BIGINT,
        error_message       STRING

    )
    PARTITIONED BY (table_id)
    LOCATION '{METADATA_LOCATION}/pipeline_runs/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """

    if table_exists("pipeline_runs"):
        logger.info("Table already exists, skipping: pipeline_runs")
    else:
        execute_query(
            pipeline_runs_sql,
            "Create pipeline_runs"
        )


    # ========================================================
    # 6. SYNC TABLE PARAMETERS
    # ========================================================
    # 3 rows, all meaningful:
    #   load_type       - read by common.py's write_by_load_type() AND
    #                     context_tracking.py's run_step_with_context() /
    #                     orchestration.py's run_pipeline() at runtime:
    #                     decides overwrite_iceberg() vs merge_iceberg(),
    #                     and whether table_watermarks advances at all
    #                     (only for non-FULL load types).
    #   primary_key     - documents the real composite/natural key (derived
    #                     from config.NATURAL_KEY_COLUMNS, so it can't
    #                     drift), matching what job_3/job_4's
    #                     resolve_duplicates() actually dedupes on.
    #   lookback_window - days to widen an incremental pull by, to catch
    #                     late-arriving source records (derived from
    #                     config.LOOKBACK_WINDOW_DAYS). Not applied by any
    #                     code yet - this pipeline is entirely load_type=
    #                     'FULL' today - but tracked so it's already in
    #                     place for whenever a table becomes incremental.
    # Dropped 'watermark_column'='updated_at': nothing reads it, and it was
    # inaccurate - this pipeline has no per-row 'updated_at' source column;
    # freshness is tracked at the RUN level via table_watermarks'
    # last_watermark_value, set once per run by orchestration.py.
    #
    # DELETE + INSERT instead of "skip if the table already had data": a
    # one-time seed can't self-correct - every time a value here turned out
    # to be wrong (load_type, primary_key), fixing the script didn't fix
    # what's already in AWS, and required a separate manual UPDATE. Syncing
    # on every run means re-running this script always reflects whatever
    # this script currently says, no manual patching needed.

    if table_parameters_existed:
        execute_query(
            f"DELETE FROM {DATABASE}.table_parameters WHERE table_id = 1",
            "Clear existing resaleflat_price table parameters before re-sync",
        )

    table_parameters_insert_sql = f"""
    INSERT INTO {DATABASE}.table_parameters
    VALUES

    (
        1,
        'load_type',
        'FULL',
        current_timestamp
    ),

    (
        1,
        'primary_key',
        '{",".join(NATURAL_KEY_COLUMNS)}',
        current_timestamp
    ),

    (
        1,
        'lookback_window',
        '{LOOKBACK_WINDOW_DAYS}',
        current_timestamp
    )
    """

    execute_query(
        table_parameters_insert_sql,
        "Sync resaleflat_price table parameters"
    )


    # ========================================================
    # 7. INITIALIZE WATERMARKS
    # ========================================================
    # Deliberately NOT synced like steps 6/8 above - these rows hold LIVE
    # pipeline progress (context_tracking.py bumps last_watermark_value
    # after every successful run). Resetting them on every setup.sh re-run
    # would erase real progress, not fix stale seed data - skip-if-exists
    # is the correct behaviour here, unlike table_parameters/metadata_tables
    # which are static reference data that's safe (and meant) to resync.

    table_watermarks_insert_sql = f"""
    INSERT INTO {DATABASE}.table_watermarks
    VALUES

    (
        1,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        2,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        3,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        5,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    ),

    (
        6,
        '1900-01-01 00:00:00',
        current_timestamp,
        NULL
    )
    """

    if table_watermarks_existed:
        logger.info("table_watermarks already had data, skipping seed insert.")
    else:
        execute_query(
            table_watermarks_insert_sql,
            "Initialize table watermarks"
        )


    # ========================================================
    # 8. SYNC resaleflat_price METADATA
    # ========================================================

    # source_path is DERIVED from config.py's SOURCE_S3_BUCKET/PREFIX - the
    # REAL S3 location job_1 writes source CSVs to and job_2 reads them back
    # from - not a hand-typed path that can point nowhere real.
    # bronze_schema/silver_schema/gold_schema hold the ACTUAL Iceberg table
    # name(s) at each medallion tier for this dataset (not the literal words
    # "bronze"/"silver"/"gold" - those were placeholders that didn't point
    # to anything real either). target_layer is the final, most-refined
    # layer this dataset is published at - 'gold', since hashed_iceberg
    # (SCD2-versioned, ready for consumption) is the last stage.
    #
    # Same DELETE + INSERT self-healing sync as table_parameters above -
    # see that section's comment for why a one-time seed isn't good enough.
    source_path = f"s3://{SOURCE_S3_BUCKET}/{SOURCE_S3_PREFIX}/"

    if metadata_tables_existed:
        execute_query(
            f"DELETE FROM {DATABASE}.metadata_tables WHERE table_id = 1",
            "Clear existing resaleflat_price metadata before re-sync",
        )

    resaleflat_price_sql = f"""
    INSERT INTO {DATABASE}.metadata_tables
    VALUES (

        1,
        'resaleflat_price',
        'HDB',
        NULL,
        'resaleflat_price',
        '{source_path}',
        'gold',
        'raw_iceberg',
        'cleaned_iceberg,transformed_iceberg',
        'hashed_iceberg',
        TRUE,
        1,
        current_timestamp

    )
    """

    execute_query(
        resaleflat_price_sql,
        "Sync resaleflat_price metadata"
    )


    # ========================================================
    # SUCCESS
    # ========================================================

    logger.info(
        "============================================================"
    )

    logger.info(
        "HDB Housing Metadata Setup Completed Successfully"
    )

    logger.info(
        "============================================================"
    )

    print("Metadata tables setup completed successfully.")  # visible even with logging silenced above


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    try:

        setup_metadata()

    except Exception:

        logger.exception(
            "HDB Housing metadata setup failed"
        )

        raise