import sys
import csv
import io
import json
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
import boto3
from awsglue.utils import getResolvedOptions

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("GluePythonshellBronzeIngestion")

# ============================================================
# GLUE PYTHONSHELL ARGUMENT RESOLUTION
# ============================================================
# Pass --CONFIG_S3_PATH as a Job Parameter in AWS Glue:
# Key: --CONFIG_S3_PATH
# Value: s3://<your-bucket>/HDB/01_template_creation/env_config/dev/dev_pipeline_config.txt
args = getResolvedOptions(sys.argv, ["CONFIG_S3_PATH"])
CONFIG_S3_PATH = args["CONFIG_S3_PATH"]

s3_client = boto3.client("s3")


# ============================================================
# 1. LOAD CONFIGURATION DIRECTLY FROM S3
# ============================================================
def load_s3_pipeline_config(s3_uri: str) -> dict:
    """Reads key-value configurations directly from S3 without local file dependencies."""
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    logger.info(f"Downloading config file from S3: {s3_uri}")
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
ATHENA_RESULTS_LOCATION = PIPELINE_CONFIG.get(
    "ATHENA_RESULTS_LOCATION",
    f"s3://{METADATA_BUCKET}/athena-results/"
)

athena_client = boto3.client("athena", region_name=REGION)


# ============================================================
# ATHENA QUERY EXECUTION HELPERS
# ============================================================
def execute_athena_sql(sql: str, description: str) -> str:
    """Executes Athena DDL/DML and waits for completion."""
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
    """Executes Athena query and returns scalar result."""
    query_id = execute_athena_sql(sql, description)
    results = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"]
    if len(rows) > 1 and rows[1]["Data"]:
        return rows[1]["Data"][0].get("VarCharValue", "0")
    return "0"


# ============================================================
# 2. RUNTIME CONTEXT INGESTION (S3 STREAMING)
# ============================================================
def load_runtime_context() -> dict:
    """Streams and parses the JSON context metadata directly from S3."""
    logger.info(f"Fetching context from s3://{METADATA_BUCKET}/{CONTEXT_KEY}")
    obj = s3_client.get_object(Bucket=METADATA_BUCKET, Key=CONTEXT_KEY)
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_runtime_context(context_data: dict):
    """Saves updated context dictionary directly back to S3."""
    s3_client.put_object(
        Bucket=METADATA_BUCKET,
        Key=CONTEXT_KEY,
        Body=json.dumps(context_data, indent=4).encode("utf-8")
    )
    logger.info(f"Updated context synced to s3://{METADATA_BUCKET}/{CONTEXT_KEY}")


def get_csv_headers_from_s3(bucket: str, s3_key: str) -> list:
    """Discovers schema dynamically by reading first chunk of target CSV."""
    response = s3_client.get_object(Bucket=bucket, Key=s3_key, Range="bytes=0-4096")
    first_chunk = response["Body"].read().decode("utf-8")
    first_line = first_chunk.splitlines()[0]
    reader = csv.reader(io.StringIO(first_line))
    raw_columns = next(reader)
    return [col.strip().lower().replace(" ", "_").replace("-", "_") for col in raw_columns if col]


# ============================================================
# 3. GLUE PYTHONSHELL INGESTION ENGINE
# ============================================================
def run_dynamic_pipeline():
    context = load_runtime_context()
    tables = context.get("tables", [])

    if not tables:
        logger.warning("No tables found in runtime context JSON.")
        return

    for table in tables:
        table_name = table["table_name"]
        target_layer = table.get("target_layer", PIPELINE_CONFIG.get("TARGET_LAYER", "bronze")).lower()
        source_path_raw = table["source_path"]
        params = table.get("parameters", {})
        
        load_type = params.get("load_type", "APPEND").upper()
        partition_cols = params.get("partition_by", [])

        logger.info(f"Starting Table: {table_name} | Layer: {target_layer} | LoadType: {load_type}")

        # Resolve Source S3 Bucket and Prefix
        if source_path_raw.startswith("s3://"):
            parsed_src = urlparse(source_path_raw)
            src_bucket = parsed_src.netloc
            src_prefix = parsed_src.path.lstrip("/")
        else:
            src_bucket = METADATA_BUCKET
            src_prefix = source_path_raw.lstrip("/")

        target_table = f"{table_name}_{target_layer}"
        staging_table = f"stg_{table_name}_{target_layer}_raw"
        target_location = f"s3://{METADATA_BUCKET}/{target_layer}/{table_name}/"

        # Validate S3 source files
        resp = s3_client.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix)
        raw_objects = resp.get("Contents", [])
        valid_files = [obj for obj in raw_objects if obj["Key"].endswith(".csv") and obj["Size"] > 0]

        if not valid_files:
            raise FileNotFoundError(f"No valid CSV files found in s3://{src_bucket}/{src_prefix}")

        file_names = [file_obj["Key"].split("/")[-1] for file_obj in valid_files]
        first_file_key = valid_files[0]["Key"]

        # Derive schema dynamically: Context schema (if defined) or CSV Header discovery
        schema_def = table.get("schemas", {}).get(target_layer)
        if schema_def and isinstance(schema_def, dict):
            columns = list(schema_def.keys())
            cols_ddl = ",\n    ".join([f"`{col}` {dtype.upper()}" for col, dtype in schema_def.items()])
        else:
            columns = get_csv_headers_from_s3(src_bucket, first_file_key)
            cols_ddl = ",\n    ".join([f"`{col}` STRING" for col in columns])

        # Create External Staging Table
        execute_athena_sql(f"DROP TABLE IF EXISTS {DATABASE}.{staging_table}", "Drop staging table")
        create_stg_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{staging_table} (
            {cols_ddl}
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 's3://{src_bucket}/{src_prefix}'
        TBLPROPERTIES ('skip.header.line.count'='1')
        """
        execute_athena_sql(create_stg_sql, f"Create staging table {staging_table}")

        # Check Record Count
        count_sql = f"SELECT COUNT(*) FROM {DATABASE}.{staging_table}"
        raw_record_count = int(get_query_scalar_result(count_sql, "Count staging records"))
        if raw_record_count == 0:
            raise ValueError(f"Staging table {staging_table} has 0 records.")

        # Target Iceberg Table DDL
        has_month_col = "month" in columns
        target_cols_ddl = cols_ddl + ",\n    `ingestion_timestamp` TIMESTAMP"
        
        partition_clause = ""
        if partition_cols:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
        elif has_month_col:
            partition_clause = "PARTITIONED BY (year)"
            target_cols_ddl += ",\n    `year` INT"

        create_iceberg_sql = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{target_table} (
            {target_cols_ddl}
        )
        {partition_clause}
        LOCATION '{target_location}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'PARQUET'
        )
        """
        execute_athena_sql(create_iceberg_sql, f"Ensure target Iceberg table {target_table} exists")

        # Dynamic Insert Query
        select_clause = ",\n    ".join([f"`{col}`" for col in columns])
        select_clause += ",\n    CURRENT_TIMESTAMP AS ingestion_timestamp"
        if has_month_col and "year" not in columns:
            select_clause += ",\n    CAST(SUBSTR(month, 1, 4) AS INT) AS year"

        if load_type == "OVERWRITE":
            execute_athena_sql(f"DELETE FROM {DATABASE}.{target_table}", f"Truncate {target_table}")

        insert_sql = f"""
        INSERT INTO {DATABASE}.{target_table}
        SELECT
            {select_clause}
        FROM {DATABASE}.{staging_table}
        """
        execute_athena_sql(insert_sql, f"Execute {load_type} load into {target_table}")
        execute_athena_sql(f"DROP TABLE IF EXISTS {DATABASE}.{staging_table}", "Drop staging table")

        # Update JSON Context in-memory
        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if "watermark" in table:
            table["watermark"]["last_updated_at"] = now_ts
        table["execution_stats"] = {
            "status": "SUCCEEDED",
            "layer": target_layer,
            "processed_at": now_ts,
            "file_count": len(valid_files),
            "processed_files": file_names,
            "record_count": raw_record_count,
            "target_table": f"{DATABASE}.{target_table}",
            "target_location": target_location,
            "load_type_applied": load_type
        }

    # Persist updated runtime context to S3
    save_runtime_context(context)
    logger.info("Glue Python Shell job completed all table operations successfully.")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    try:
        run_dynamic_pipeline()
    except Exception as e:
        logger.exception("Glue Python Shell Job execution failed.")
        sys.exit(1)