import json
import os
import urllib.parse
import boto3


glue_client = boto3.client("glue")


BRONZE_JOB_NAME = os.environ["BRONZE_JOB_NAME"]


def lambda_handler(event, context):

    print("========================================")
    print("HDB INGESTION LAMBDA STARTED")
    print("========================================")

    print("Received event:")
    print(json.dumps(event))

    # Get S3 object information
    record = event["Records"][0]

    bucket_name = record["s3"]["bucket"]["name"]

    source_key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"]
    )

    print(f"Source bucket : {bucket_name}")
    print(f"Source key    : {source_key}")

    # Start Bronze Glue job
    response = glue_client.start_job_run(
        JobName=BRONZE_JOB_NAME,
        Arguments={
            "--SOURCE_KEY": source_key,
            "--RAW_BUCKET": bucket_name
        }
    )

    job_run_id = response["JobRunId"]

    print(f"Bronze Glue Job : {BRONZE_JOB_NAME}")
    print(f"Glue Job Run ID : {job_run_id}")

    print("========================================")
    print("LAMBDA COMPLETED")
    print("========================================")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Bronze Glue job started successfully",
            "bucket": bucket_name,
            "source_key": source_key,
            "job_run_id": job_run_id
        })
    }