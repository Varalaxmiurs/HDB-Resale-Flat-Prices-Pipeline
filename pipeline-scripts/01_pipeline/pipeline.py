import boto3
import os
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client("athena")


# =====================================================
# CONFIGURATION
# =====================================================

DB_NAME = os.environ["DB_NAME"]
WORKGROUP = os.environ["WORKGROUP"]
S3_BUCKET = os.environ["S3_BUCKET"]

BRONZE_PATH = os.environ["BRONZE_PATH"]
SILVER_PATH = os.environ["SILVER_PATH"]
GOLD_PATH = os.environ["GOLD_PATH"]


BRONZE_LOCATION = f"s3://{S3_BUCKET}/{BRONZE_PATH}"
SILVER_LOCATION = f"s3://{S3_BUCKET}/{SILVER_PATH}"
GOLD_LOCATION = f"s3://{S3_BUCKET}/{GOLD_PATH}"


# =====================================================
# RUN ATHENA QUERY
# =====================================================

def run_query(sql, desc):

    logger.info(f"Running: {desc}")

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": DB_NAME
        },
        WorkGroup=WORKGROUP
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info(
        f"QueryExecutionId: {query_execution_id}"
    )

    return query_execution_id


# =====================================================
# WAIT FOR QUERY
# =====================================================

def wait_for_query(query_execution_id):

    while True:

        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = response["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":

            logger.info(
                f"Query succeeded: {query_execution_id}"
            )

            return

        if status in ["FAILED", "CANCELLED"]:

            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown error"
            )

            raise Exception(reason)

        time.sleep(2)


# =====================================================
# BRONZE TABLE
# =====================================================

bronze_sql = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.bronze_resale_flat (

    resale_id STRING,
    customer_id STRING,
    product_id STRING,
    resale_date STRING,
    resale_amount DOUBLE,
    raw_file STRING

)
LOCATION '{BRONZE_LOCATION}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET'
)
"""


# =====================================================
# SILVER TABLE
# =====================================================

silver_sql = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.silver_resale_flat (

    resale_id STRING,
    customer_id STRING,
    product_id STRING,
    resale_date DATE,
    resale_amount DECIMAL(18,2),
    load_timestamp TIMESTAMP

)
LOCATION '{SILVER_LOCATION}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET'
)
"""


# =====================================================
# GOLD TABLE
# =====================================================

gold_sql = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.gold_resale_flat_summary (

    customer_id STRING,
    total_resale_amount DECIMAL(18,2),
    resale_count BIGINT,
    last_resale_date DATE

)
LOCATION '{GOLD_LOCATION}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET'
)
"""


# =====================================================
# LAMBDA HANDLER
# =====================================================

def lambda_handler(event, context):

    try:

        # Bronze
        query_id = run_query(
            bronze_sql,
            "Create Bronze Iceberg Table"
        )
        wait_for_query(query_id)


        # Silver
        query_id = run_query(
            silver_sql,
            "Create Silver Iceberg Table"
        )
        wait_for_query(query_id)


        # Gold
        query_id = run_query(
            gold_sql,
            "Create Gold Iceberg Table"
        )
        wait_for_query(query_id)


        logger.info(
            "Bronze, Silver and Gold Iceberg tables created successfully"
        )

        return {
            "statusCode": 200,
            "body": "All Iceberg tables created successfully"
        }


    except Exception as e:

        logger.exception(
            "Failed to create Iceberg tables"
        )

        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }