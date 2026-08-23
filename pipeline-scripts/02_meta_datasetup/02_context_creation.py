import os
import time
import uuid
import json
import logging
import argparse
from datetime import datetime
import boto3
import pandas as pd

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ContextBuilder")


# ============================================================
# CONTEXT BUILDER CLASS
# ============================================================
class PipelineContextBuilder:
    def __init__(self, database: str, workgroup: str, metadata_bucket: str, region: str):
        self.database = database
        self.workgroup = workgroup
        self.metadata_bucket = metadata_bucket
        self.athena_results = f"s3://{metadata_bucket}/athena-results/"
        
        self.session = boto3.Session(region_name=region)
        self.athena = self.session.client("athena")
        self.s3 = self.session.client("s3")
        self.run_id = int(datetime.utcnow().strftime("%Y%m%d%H%M%S"))

    def execute_query(self, sql: str, description: str) -> pd.DataFrame:
        """Executes an Athena query synchronously and loads results into a DataFrame."""
        logger.info(f"Executing: {description}")
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.athena_results},
            WorkGroup=self.workgroup
        )
        query_id = response["QueryExecutionId"]

        while True:
            exec_state = self.athena.get_query_execution(QueryExecutionId=query_id)
            status = exec_state["QueryExecution"]["Status"]["State"]

            if status == "SUCCEEDED":
                result_csv_uri = f"{self.athena_results.rstrip('/')}/{query_id}.csv"
                return pd.read_csv(result_csv_uri)
            elif status in ["FAILED", "CANCELLED"]:
                reason = exec_state["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                raise RuntimeError(f"Athena query failed [{status}]: {reason}\nSQL: {sql}")

            time.sleep(1)

    def extract_metadata_context(self) -> dict:
        """Joins metadata_tables, table_parameters, and table_watermarks into a context dictionary."""
        # 1. Fetch active tables ordered by load_order
        tables_sql = f"""
        SELECT 
            table_id, table_name, source_system, source_schema, source_table,
            source_path, target_layer, bronze_schema, silver_schema, gold_schema,
            load_order
        FROM {self.database}.metadata_tables
        WHERE active_flag = TRUE
        ORDER BY load_order ASC
        """
        tables_df = self.execute_query(tables_sql, "Fetch active metadata tables")

        # 2. Fetch parameters
        params_sql = f"SELECT table_id, parameter_name, parameter_value FROM {self.database}.table_parameters"
        params_df = self.execute_query(params_sql, "Fetch table parameters")

        # 3. Fetch watermarks
        watermarks_sql = f"SELECT table_id, last_watermark_value, last_updated_at, last_run_id FROM {self.database}.table_watermarks"
        watermarks_df = self.execute_query(watermarks_sql, "Fetch table watermarks")

        # 4. Construct Context Structure
        context = {
            "pipeline_execution": {
                "run_id": self.run_id,
                "database": self.database,
                "execution_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "total_active_tables": len(tables_df)
            },
            "tables": []
        }

        for _, row in tables_df.iterrows():
            t_id = row["table_id"]

            # Filter parameters for this table
            t_params_df = params_df[params_df["table_id"] == t_id]
            parameters_dict = dict(zip(t_params_df["parameter_name"], t_params_df["parameter_value"]))

            # Filter watermark for this table
            t_wm_df = watermarks_df[watermarks_df["table_id"] == t_id]
            watermark_val = str(t_wm_df["last_watermark_value"].iloc[0]) if not t_wm_df.empty else "1900-01-01 00:00:00"
            last_run_id = int(t_wm_df["last_run_id"].iloc[0]) if not t_wm_df.empty and pd.notna(t_wm_df["last_run_id"].iloc[0]) else None

            table_meta = {
                "table_id": int(t_id),
                "table_name": row["table_name"],
                "load_order": int(row["load_order"]),
                "source_system": row["source_system"],
                "source_path": row["source_path"],
                "target_layer": row["target_layer"],
                "schemas": {
                    "source": row["source_schema"] if pd.notna(row["source_schema"]) else None,
                    "bronze": row["bronze_schema"],
                    "silver": row["silver_schema"],
                    "gold": row["gold_schema"]
                },
                "watermark": {
                    "last_watermark_value": watermark_val,
                    "watermark_column": parameters_dict.get("watermark_column", "updated_at"),
                    "last_run_id": last_run_id
                },
                "parameters": parameters_dict
            }
            context["tables"].append(table_meta)

        logger.info("Pipeline context dictionary successfully extracted and built.")
        return context


# ============================================================
# EXECUTION ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Data-Driven Pipeline Context JSON Object")
    parser.add_argument("--database", required=True, help="Glue/Athena database name")
    parser.add_argument("--workgroup", required=True, help="Athena workgroup")
    parser.add_argument("--metadata-bucket", required=True, help="S3 metadata bucket")
    parser.add_argument("--region", required=True, help="AWS region")

    args = parser.parse_args()

    builder = PipelineContextBuilder(
        database=args.database,
        workgroup=args.workgroup,
        metadata_bucket=args.metadata_bucket,
        region=args.region
    )

    # Returns the complete JSON-serializable dictionary directly
    context_json: dict = builder.extract_metadata_context()
    
    # You can now print or pass 'context_json' directly into your application code
    print(json.dumps(context_json, indent=2))