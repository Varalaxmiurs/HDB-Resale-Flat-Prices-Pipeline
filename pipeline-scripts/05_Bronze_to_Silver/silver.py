import sys
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
import boto3
from awsglue.utils import getResolvedOptions

# Import the Silver DQ Validation module
from dataquality_checks import silver_validation

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("GlueSilverTransformationWithDQ")

# ============================================================
# GLUE ARGUMENT RESOLUTION
# ============================================================
# Provide --CONFIG_S3_PATH in AWS Glue Job parameters:
# --CONFIG_S3_PATH = s3://<bucket>/HDB/01_template_creation/env_config/dev/dev_pipeline_config.txt
args = getResolvedOptions(sys.argv, ["CONFIG_S3_PATH"])
CONFIG_S3_PATH = args["CONFIG_S3_PATH"]

s3_client = boto3.client("s3")


# ============================================================
# CONFIG LOADER FROM S3
# ============================================================
def load_s3_pipeline_config(s3_uri: str) -> dict:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    logger.info(f"Loading config from S3: {s3_uri}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    config = {}
    for line in content.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            config[k.strip()] = v.strip().strip('"').strip("'")
    return config


PIPELINE_CONFIG = load_s3_pipeline_config(CONFIG_S3_PATH)

DATABASE = PIPELINE_CONFIG["DATABASE_NAME"]
WORKGROUP = PIPELINE_CONFIG["ATHENA_WORKGROUP"]
METADATA_BUCKET = PIPELINE_CONFIG["METADATA_BUCKET"]
REGION = PIPELINE_CONFIG["AWS_REGION"]
CONTEXT_KEY = PIPELINE_CONFIG["CONTEXT_KEY"]
SILVER_LAYER = PIPELINE_CONFIG.get("SILVER_TARGET_LAYER", "silver").lower()
BRONZE_LAYER = PIPELINE_CONFIG.get("BRONZE_TARGET_LAYER", "bronze").lower()
FAILED_TABLE_NAME = PIPELINE_CONFIG.get("FAILED_RECORDS_TABLE", f"{SILVER_LAYER}_failed_records")

ATHENA_RESULTS_LOCATION = PIPELINE_CONFIG.get(
    "ATHENA_RESULTS_LOCATION",
    f"s3://{METADATA_BUCKET}/athena-results/"
)

athena_client = boto3.client("athena", region_name=REGION)


# ============================================================
# ATHENA EXECUTION ENGINE
# ============================================================
def execute_athena_sql(sql: str, description: str) -> str:
    logger.info(f"Executing: {description}")
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
        WorkGroup=WORKGROUP
    )
    query_id = response["QueryExecutionId"]

    while True:
        res = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = res["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return query_id
        elif state in ["FAILED", "CANCELLED"]:
            reason = res["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise RuntimeError(f"Athena SQL Failed [{state}]: {reason}\nQuery:\n{sql}")
        time.sleep(2)


def get_query_scalar_result(sql: str, description: str) -> str:
    query_id = execute_athena_sql(sql, description)
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"]
    if len(rows) > 1 and rows[1]["Data"]:
        return rows[1]["Data"][0].get("VarCharValue", "0")
    return "0"


# ============================================================
# S3 CONTEXT STATE MANAGER
# ============================================================
def load_runtime_context() -> dict:
    logger.info(f"Streaming context JSON from s3://{METADATA_BUCKET}/{CONTEXT_KEY}")
    obj = s3_client.get_object(Bucket=METADATA_BUCKET, Key=CONTEXT_KEY)
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_runtime_context(context_data: dict):
    s3_client.put_object(
        Bucket=METADATA_BUCKET,
        Key=CONTEXT_KEY,
        Body=json.dumps(context_data, indent=4).encode("utf-8")
    )
    logger.info(f"Context JSON synchronized to s3://{METADATA_BUCKET}/{CONTEXT_KEY}")


# ============================================================
# BRONZE-TO-SILVER PIPELINE WITH DQ CHECKS
# ============================================================
def run_silver_pipeline():
    context = load_runtime_context()
    tables = context.get("tables", [])

    if not tables:
        logger.warning("No tables found in runtime context.")
        return

    # Ensure silver.failed table exists
    failed_table_location = f"s3://{METADATA_BUCKET}/{SILVER_LAYER}/failed_records/"
    create_failed_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{FAILED_TABLE_NAME} (
        `failed_table_name` STRING,
        `failed_at` TIMESTAMP,
        `failure_reason` STRING
    )
    PARTITIONED BY (failed_table_name)
    LOCATION '{failed_table_location}'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'PARQUET'
    )
    """
    execute_athena_sql(create_failed_table_sql, "Ensure failed records table exists")

    for table in tables:
        table_name = table["table_name"]
        params = table.get("parameters", {})
        silver_schema = table.get("schemas", {}).get(SILVER_LAYER, {})
        dq_rules = table.get("dq_checks", {})
        primary_keys = params.get("primary_keys", [])
        partition_cols = params.get("partition_by", [])
        load_type = params.get("load_type", "MERGE").upper()
        
        # Read Baseline Bronze Count from Context
        expected_bronze_count = table.get("execution_stats", {}).get("record_count", 0)

        if not silver_schema:
            logger.warning(f"Skipping {table_name}: No silver schema in context JSON.")
            continue

        source_bronze_table = f"{table_name}_{BRONZE_LAYER}"
        target_silver_table = f"{table_name}_{SILVER_LAYER}"
        target_silver_location = f"s3://{METADATA_BUCKET}/{SILVER_LAYER}/{table_name}/"

        logger.info(f"Starting Silver Load: {source_bronze_table} -> {target_silver_table}")

        # 1. Build Column Definitions and Explicit Casting
        silver_col_defs = []
        select_expressions = []
        for col, dtype in silver_schema.items():
            silver_col_defs.append(f"`{col}` {dtype.upper()}")
            select_expressions.append(f"CAST(`{col}` AS {dtype.upper()}) AS `{col}`")

        silver_col_defs.append("`silver_processed_at` TIMESTAMP")
        select_expressions.append("CURRENT_TIMESTAMP AS silver_processed_at")

        # Partition setup
        partition_clause = ""
        if partition_cols:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
            for p_col in partition_cols:
                if p_col not in silver_schema:
                    silver_col_defs.append(f"`{p_col}` INT")

        # 2. Ensure Target Silver Table Exists
        create_silver_sql = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{target_silver_table} (
            {', '.join(silver_col_defs)}
        )
        {partition_clause}
        LOCATION '{target_silver_location}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'PARQUET'
        )
        """
        execute_athena_sql(create_silver_sql, f"Ensure target table {target_silver_table} exists")

        # 3. Deduplication Query on Bronze Source
        if primary_keys:
            pk_str = ", ".join([f"`{pk}`" for pk in primary_keys])
            source_query = f"""
                SELECT {', '.join(select_expressions)}
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(PARTITION BY {pk_str} ORDER BY ingestion_timestamp DESC) as rn
                    FROM {DATABASE}.{source_bronze_table}
                ) deduplicated
                WHERE rn = 1
            """
        else:
            source_query = f"""
                SELECT {', '.join(select_expressions)}
                FROM {DATABASE}.{source_bronze_table}
            """

        # 4. Perform Data Load (MERGE / OVERWRITE / APPEND)
        if load_type == "MERGE" and primary_keys:
            join_cond = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in primary_keys])
            update_set = ", ".join([f"`{col}` = source.`{col}`" for col in silver_schema.keys()])
            update_set += ", `silver_processed_at` = source.silver_processed_at"
            
            insert_cols = ", ".join([f"`{col}`" for col in silver_schema.keys()] + ["`silver_processed_at`"])
            insert_vals = ", ".join([f"source.`{col}`" for col in silver_schema.keys()] + ["source.silver_processed_at"])

            merge_sql = f"""
            MERGE INTO {DATABASE}.{target_silver_table} AS target
            USING ({source_query}) AS source
            ON ({join_cond})
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            execute_athena_sql(merge_sql, f"Execute MERGE into {target_silver_table}")

        elif load_type == "OVERWRITE":
            execute_athena_sql(f"DELETE FROM {DATABASE}.{target_silver_table}", f"Truncate {target_silver_table}")
            insert_sql = f"INSERT INTO {DATABASE}.{target_silver_table} {source_query}"
            execute_athena_sql(insert_sql, f"Execute OVERWRITE into {target_silver_table}")

        else:
            insert_sql = f"INSERT INTO {DATABASE}.{target_silver_table} {source_query}"
            execute_athena_sql(insert_sql, f"Execute APPEND into {target_silver_table}")

        # 5. Execute Data Quality Checks via Imported Module
        is_valid, dq_report = silver_validation(
            table_name=table_name,
            database=DATABASE,
            bronze_table=source_bronze_table,
            silver_table=target_silver_table,
            silver_failed_table=FAILED_TABLE_NAME,
            silver_schema=silver_schema,
            primary_keys=primary_keys,
            dq_rules=dq_rules,
            expected_bronze_count=expected_bronze_count,
            query_executor_fn=execute_athena_sql,
            scalar_executor_fn=get_query_scalar_result
        )

        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # 6. Update Runtime Context JSON with Execution & DQ Metrics
        table["silver_execution_stats"] = {
            "status": "SUCCEEDED" if is_valid else "FAILED_DQ_CHECKS",
            "layer": SILVER_LAYER,
            "processed_at": now_ts,
            "source_table": f"{DATABASE}.{source_bronze_table}",
            "target_table": f"{DATABASE}.{target_silver_table}",
            "target_location": target_silver_location,
            "load_type_applied": load_type,
            "data_quality_report": dq_report
        }

        if not is_valid:
            save_runtime_context(context)
            raise ValueError(f"Silver Data Quality Checks Failed for table '{table_name}'. Report: {json.dumps(dq_report)}")

    save_runtime_context(context)
    logger.info("Silver transformation and data quality validations completed successfully.")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    try:
        run_silver_pipeline()
    except Exception as e:
        logger.exception("Silver Glue Pipeline execution halted.")
        sys.exit(1)