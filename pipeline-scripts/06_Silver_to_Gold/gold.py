import sys
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
logger = logging.getLogger("GlueGoldTransformationAndReconciliation")

# ============================================================
# GLUE ARGUMENT RESOLUTION
# ============================================================
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

GOLD_LAYER = PIPELINE_CONFIG.get("GOLD_TARGET_LAYER", "gold").lower()
SILVER_LAYER = PIPELINE_CONFIG.get("SILVER_TARGET_LAYER", "silver").lower()

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
# SILVER-TO-GOLD PIPELINE, RECONCILIATION & DQ ENGINE
# ============================================================
def run_gold_pipeline():
    context = load_runtime_context()
    tables = context.get("tables", [])

    if not tables:
        logger.warning("No tables found in runtime context.")
        return

    # Filter tables designated for Gold layer transformation
    gold_tables = [
        t for t in tables
        if t.get("target_layer", "").lower() == GOLD_LAYER
        or t.get("schemas", {}).get(GOLD_LAYER) is not None
    ]

    if not gold_tables:
        logger.warning(f"No tables configured for Gold layer target: {GOLD_LAYER}")
        return

    for table in gold_tables:
        table_name = table["table_name"]
        params = table.get("parameters", {})
        gold_schema = table.get("schemas", {}).get(GOLD_LAYER, {})
        primary_keys = params.get("primary_keys", [])
        partition_cols = params.get("partition_by", [])
        load_type = params.get("load_type", "MERGE").upper()

        source_silver_table = f"{table_name}_{SILVER_LAYER}"
        target_gold_table = f"{table_name}_{GOLD_LAYER}"
        target_gold_location = f"s3://{METADATA_BUCKET}/{GOLD_LAYER}/{table_name}/"

        logger.info(f"Starting Custom Gold Transformation & Reconciliation: {source_silver_table} -> {target_gold_table}")

        # 1. Build Gold Column DDL and Select Expressions
        gold_col_defs = []
        select_expressions = []
        
        for col, dtype in gold_schema.items():
            gold_col_defs.append(f"`{col}` {dtype.upper()}")
            select_expressions.append(f"CAST(`{col}` AS {dtype.upper()}) AS `{col}`")

        gold_col_defs.append("`gold_processed_at` TIMESTAMP")
        select_expressions.append("CURRENT_TIMESTAMP AS gold_processed_at")

        # Partition setup
        partition_clause = ""
        if partition_cols:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
            for p_col in partition_cols:
                if p_col not in gold_schema:
                    gold_col_defs.append(f"`{p_col}` INT")

        # 2. Ensure Target Gold Iceberg Table Exists
        create_gold_sql = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{target_gold_table} (
            {', '.join(gold_col_defs)}
        )
        {partition_clause}
        LOCATION '{target_gold_location}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'PARQUET'
        )
        """
        execute_athena_sql(create_gold_sql, f"Ensure target Gold table {target_gold_table} exists")

        # 3. Custom Deduplication Subquery (Requirement 2: higher price preferred)
        deduplicated_silver_query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER(
                           PARTITION BY block, town, flat_type, month 
                           ORDER BY resale_price DESC
                       ) as price_rank
                FROM {DATABASE}.{source_silver_table}
            )
            WHERE price_rank = 1
        """

        # 4. Custom Resale Identifier & SHA-256 Hashing Subquery (Requirements 1 & 3)
        gold_transformation_query = f"""
        WITH deduped AS ({deduplicated_silver_query}),
        avg_price_calc AS (
            SELECT 
                town, 
                flat_type, 
                month,
                SUBSTR(CAST(ROUND(AVG(resale_price)) AS VARCHAR), 1, 2) as avg_price_prefix
            FROM deduped
            GROUP BY town, flat_type, month
        ),
        raw_identifier AS (
            SELECT 
                d.*,
                CONCAT(
                    'S',
                    LPAD(REGEXP_REPLACE(d.block, '[^0-9]', ''), 3, '0'),
                    a.avg_price_prefix,
                    SUBSTR(d.month, 6, 2),
                    SUBSTR(d.town, 1, 1)
                ) AS generated_resale_id
            FROM deduped d
            JOIN avg_price_calc a 
              ON d.town = a.town 
             AND d.flat_type = a.flat_type 
             AND d.month = a.month
        )
        SELECT 
            {', '.join([f"d_final.`{col}`" for col in gold_schema.keys() if col not in ['generated_resale_id', 'resale_identifier_hash']])},
            -- Include custom business columns if expected in schema
            {', '.join([f"CAST(d_final.{col} AS {gold_schema[col].upper()}) AS `{col}`" for col in ['generated_resale_id', 'resale_identifier_hash'] if col in gold_schema])}
        FROM (
            SELECT 
                *,
                TO_HEX(SHA2(CAST(generated_resale_id AS VARBINARY), 256)) AS resale_identifier_hash
            FROM raw_identifier
        ) d_final
        """

        # 5. Execute Load Strategy (MERGE / OVERWRITE / APPEND)
        if load_type == "MERGE" and primary_keys:
            join_cond = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in primary_keys])
            update_set = ", ".join([f"`{col}` = source.`{col}`" for col in gold_schema.keys()])
            update_set += ", `gold_processed_at` = source.gold_processed_at"
            
            insert_cols = ", ".join([f"`{col}`" for col in gold_schema.keys()] + ["`gold_processed_at`"])
            insert_vals = ", ".join([f"source.`{col}`" for col in gold_schema.keys()] + ["source.gold_processed_at"])

            merge_sql = f"""
            MERGE INTO {DATABASE}.{target_gold_table} AS target
            USING ({gold_transformation_query}) AS source
            ON ({join_cond})
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            """
            execute_athena_sql(merge_sql, f"Execute MERGE into {target_gold_table}")

        elif load_type == "OVERWRITE":
            execute_athena_sql(f"DELETE FROM {DATABASE}.{target_gold_table}", f"Truncate {target_gold_table}")
            insert_sql = f"INSERT INTO {DATABASE}.{target_gold_table} {gold_transformation_query}"
            execute_athena_sql(insert_sql, f"Execute OVERWRITE into {target_gold_table}")

        else:
            insert_sql = f"INSERT INTO {DATABASE}.{target_gold_table} {gold_transformation_query}"
            execute_athena_sql(insert_sql, f"Execute APPEND into {target_gold_table}")

        # 6. Pre-Reconciliation & Data Quality Checks
        silver_count_sql = f"SELECT COUNT(*) FROM {DATABASE}.{source_silver_table}"
        silver_record_count = int(get_query_scalar_result(silver_count_sql, "Count silver source records"))

        gold_count_sql = f"SELECT COUNT(*) FROM {DATABASE}.{target_gold_table}"
        gold_record_count = int(get_query_scalar_result(gold_count_sql, "Count gold target records"))

        # Checksum Reconciliation
        hash_col_target = list(gold_schema.keys())[0] if gold_schema else "1"
        silver_hash_sql = f"SELECT MD5(CAST(SUM(CAST(MD5(CAST(COALESCE(CAST({hash_col_target} AS VARCHAR), '')) AS VARBINARY)) AS BIGINT)) AS VARCHAR)) FROM {DATABASE}.{source_silver_table}"
        gold_hash_sql = f"SELECT MD5(CAST(SUM(CAST(MD5(CAST(COALESCE(CAST({hash_col_target} AS VARCHAR), '')) AS VARBINARY)) AS BIGINT)) AS VARCHAR)) FROM {DATABASE}.{target_gold_table}"
        
        silver_checksum = get_query_scalar_result(silver_hash_sql, "Generate Silver checksum hash")
        gold_checksum = get_query_scalar_result(gold_hash_sql, "Generate Gold checksum hash")

        reconciliation_passed = (silver_checksum == gold_checksum)

        reconciliation_report = {
            "table_name": table_name,
            "silver_record_count": silver_record_count,
            "gold_record_count": gold_record_count,
            "silver_checksum": silver_checksum,
            "gold_checksum": gold_checksum,
            "reconciliation_status": "PASSED" if reconciliation_passed else "FAILED"
        }

        logger.info(f"Reconciliation Report for {table_name}: {json.dumps(reconciliation_report)}")

        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # 7. Update Runtime Context JSON
        table["gold_execution_stats"] = {
            "status": "SUCCEEDED" if reconciliation_passed else "FAILED_RECONCILIATION",
            "layer": GOLD_LAYER,
            "processed_at": now_ts,
            "source_table": f"{DATABASE}.{source_silver_table}",
            "target_table": f"{DATABASE}.{target_gold_table}",
            "target_location": target_gold_location,
            "load_type_applied": load_type,
            "reconciliation_report": reconciliation_report
        }

        if not reconciliation_passed:
            save_runtime_context(context)
            raise ValueError(f"Gold Layer Reconciliation Failed for '{table_name}': Checksum Mismatch.")

    save_runtime_context(context)
    logger.info("Gold layer transformation, hashing reconciliation, and context updates completed successfully.")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    try:
        run_gold_pipeline()
    except Exception as e:
        logger.exception("Gold Glue Python Shell Job execution failed.")
        sys.exit(1)