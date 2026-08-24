"""
03_parameter_store_setup.py
=============================
One-time (idempotent) setup script: pushes the pipeline's plain, NON-secret
runtime config - today, just the data.gov.sg ingestion API base URLs - into
AWS Systems Manager Parameter Store, under SSM_PARAMETER_PREFIX
(config.py's default: /hdb-pipeline).

What this deliberately does NOT store:
  - The AWS account id. Never written here, never hardcoded in source -
    common.py's get_account_id() resolves it live via STS on every run
    (sts:GetCallerIdentity), so the code stays account-agnostic and nothing
    account-identifying ever sits in this public GitHub repo.
  - The SNS topic ARN. common.py's send_alert() builds it at call time from
    AWS_REGION + SNS_TOPIC_NAME + the live account id - never a stored
    literal ARN.
  - Any actual credential/API key. data.gov.sg's collection/dataset APIs
    are public today - no Authorization header is sent anywhere (see
    job_1_ingestion_to_source.py). If that ever changes, the new API key
    belongs in Secrets Manager (see common.py's get_secret()), not here -
    Parameter Store is for plain config, not secrets.

Same idempotent-resync philosophy as 01_metadata_setup.py's
table_parameters/metadata_tables sync: this ALWAYS overwrites
(Overwrite=True) rather than skip-if-exists, so re-running after a value
changes (e.g. data.gov.sg's API domain changes) fixes Parameter Store too -
no manual `aws ssm put-parameter` needed on the side.

Usage:
    python 03_parameter_store_setup.py --region us-east-1

    # Optional overrides (defaults match config.py's own fallback values):
    python 03_parameter_store_setup.py --region us-east-1 \\
        --collection-api-base https://api-production.data.gov.sg/v2/public/api \\
        --dataset-api-base https://api-open.data.gov.sg/v1/public/api
"""

import argparse

import boto3

parser = argparse.ArgumentParser(description="HDB pipeline - Parameter Store setup (plain config only, no secrets)")
parser.add_argument("--region", required=True, help="AWS region")
parser.add_argument("--prefix", default="/hdb-pipeline", help="SSM parameter name prefix (must match config.py's SSM_PARAMETER_PREFIX / HDB_SSM_PREFIX)")
parser.add_argument("--collection-api-base", default="https://api-production.data.gov.sg/v2/public/api")
parser.add_argument("--dataset-api-base", default="https://api-open.data.gov.sg/v1/public/api")
args = parser.parse_args()

ssm = boto3.client("ssm", region_name=args.region)

PARAMETERS = {
    f"{args.prefix}/collection_api_base": args.collection_api_base,
    f"{args.prefix}/dataset_api_base": args.dataset_api_base,
}

print("============================================================")
print("HDB Pipeline - Parameter Store Setup")
print(f"Region : {args.region}")
print(f"Prefix : {args.prefix}")
print("============================================================")

for name, value in PARAMETERS.items():
    ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)
    print(f"  {name} = {value}")

print("Parameter Store setup complete.")
print(
    "Note: the AWS account id and the SNS topic ARN are intentionally NOT "
    "stored here or anywhere else - they're resolved live at runtime. See "
    "common.py's get_account_id() and send_alert()."
)
