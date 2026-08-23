import boto3
import time
import logging
import argparse


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
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


# ============================================================
# METADATA LOCATION
# ============================================================

METADATA_LOCATION = (
    f"s3://{METADATA_BUCKET}/"
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

    execute_query(
        pipeline_runs_sql,
        "Create pipeline_runs"
    )


    # ========================================================
    # 6. INSERT TABLE PARAMETERS
    # ========================================================

    table_parameters_insert_sql = f"""
    INSERT INTO {DATABASE}.table_parameters
    VALUES

    (
        1,
        'load_type',
        'MERGE',
        current_timestamp
    ),

    (
        1,
        'primary_key',
        'month,town,flat_type,block,street_name,storey_range',
        current_timestamp
    ),

    (
        1,
        'watermark_column',
        'updated_at',
        current_timestamp
    )
    """

    execute_query(
        table_parameters_insert_sql,
        "Insert resaleflat_price table parameters"
    )


    # ========================================================
    # 7. INITIALIZE WATERMARKS
    # ========================================================

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

    execute_query(
        table_watermarks_insert_sql,
        "Initialize table watermarks"
    )


    # ========================================================
    # 8. INSERT resaleflat_price METADATA
    # ========================================================

    resaleflat_price_sql = f"""
    INSERT INTO {DATABASE}.metadata_tables
    VALUES (

        1,
        'resaleflat_price',
        'HDB',
        NULL,
        'resaleflat_price',
        'raw/data/resaleflat_price/',
        'gold',
        'bronze',
        'silver',
        'gold',
        TRUE,
        1,
        current_timestamp

    )
    """

    execute_query(
        resaleflat_price_sql,
        "Insert resaleflat_price metadata"
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