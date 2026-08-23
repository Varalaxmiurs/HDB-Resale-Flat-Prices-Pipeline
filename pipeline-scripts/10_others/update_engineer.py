import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--account-id", required=True)
parser.add_argument("--region", required=True)

args = parser.parse_args()

ACCOUNT_ID = args.account_id
REGION = args.region
