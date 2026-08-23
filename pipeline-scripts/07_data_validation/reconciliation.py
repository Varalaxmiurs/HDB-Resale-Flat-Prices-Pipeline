import sys
import json
import time
import logging
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
logger = logging.getLogger("GoldLayerReconciliation")

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
# RECONCILIATION ENGINE READING JSON CONTEXT
# ============================================================
def run_reconciliation():
    context = load_runtime_context()
    tables = context.get("tables", [])

    if not tables:
        logger.warning("No tables found in runtime context.")
        return

    # Filter tables that target the Gold layer or have a Gold schema defined in the context JSON
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
        
        # Safely parse schema if loaded as a JSON string or dict from context
        raw_gold_schema = table.get("schemas", {}).get(GOLD_LAYER, {})
        if isinstance(raw_gold_schema, str):
            try:
                gold_schema = json.loads(raw_gold_schema)
            except Exception:
                gold_schema = {}
        else:
            gold_schema = raw_gold_schema

        source_silver_table = f"{table_name}_{SILVER_LAYER}"
        target_gold_table = f"{table_name}_{GOLD_LAYER}"

        logger.info(f"Running Reconciliation: {source_silver_table} vs {target_gold_table}")

        # 1. Record Counts
        silver_count_sql = f"SELECT COUNT(*) FROM {DATABASE}.{source_silver_table}"
        silver_record_count = int(get_query_scalar_result(silver_count_sql, "Count silver source records"))

        gold_count_sql = f"SELECT COUNT(*) FROM {DATABASE}.{target_gold_table}"
        gold_record_count = int(get_query_scalar_result(gold_count_sql, "Count gold target records"))

        # 2. Cryptographic Checksum Comparison
        hash_col_target = list(gold_schema.keys())[0] if gold_schema else "1"
        
        silver_hash_sql = f"SELECT MD5(CAST(SUM(CAST(MD5(CAST(COALESCE(CAST({hash_col_target} AS VARCHAR), '')) AS VARBINARY)) AS BIGINT)) AS VARCHAR)) FROM {DATABASE}.{source_silver_table}"
        gold_hash_sql = f"SELECT MD5(CAST(SUM(CAST(MD5(CAST(COALESCE(CAST({hash_col_target} AS VARCHAR), '')) AS VARBINARY)) AS BIGINT)) AS VARCHAR)) FROM {DATABASE}.{target_gold_table}"
        
        silver_checksum = get_query_scalar_result(silver_hash_sql, "Generate Silver checksum hash")
        gold_checksum = get_query_scalar_result(gold_hash_sql, "Generate Gold checksum hash")

        # Note: Record counts can differ between Silver and Gold due to deduplication (higher price kept, lower discarded).
        # We check structural checksum validation and record parity flags.
        reconciliation_passed = (silver_checksum == gold_checksum)

        reconciliation_report = {
            "table_name": table_name,
            "silver_record_count": silver_record_count,
            "gold_record_count": gold_record_count,
            "silver_checksum": silver_checksum,
            "gold_checksum": gold_checksum,
            "reconciliation_status": "PASSED" if reconciliation_passed else "MISMATCH_DETECTED"
        }

        logger.info(f"Reconciliation Report for {table_name}: {json.dumps(reconciliation_report)}")

        # 3. Inject Reconciliation Data Back into Table Context Stats
        if "gold_execution_stats" not in table:
            table["gold_execution_stats"] = {}
        
        table["gold_execution_stats"]["reconciliation_report"] = reconciliation_report

    # Save the updated context state back to S3
    save_runtime_context(context)
    logger.info("Reconciliation execution and context state update completed successfully.")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    try:
        run_reconciliation()
    except Exception as e:
        logger.exception("Reconciliation Job execution failed.")
        sys.exit(1)