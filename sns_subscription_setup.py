"""
sns_subscription_setup.py
==========================
One-time setup script: subscribes one or more real email addresses to the
SNS topic common.py's send_alert() publishes pipeline success/failure
summaries to.

Lives at the hdb_1/ project root (moved out of
pipeline-scripts/02_meta_datasetup/) so it's a one-hop command alongside
setup.sh and run_pipeline.py, not three folders deep.

WHY THIS IS A SEPARATE STEP FROM THE PIPELINE ITSELF:
Publishing a message to an SNS topic always succeeds even if nobody is
subscribed to it - "Alert sent" in the pipeline's own logs only means the
publish call succeeded, NOT that any human actually received an email.
setup.sh creates the topic but never subscribes anyone to it by default -
this script is what actually connects a real inbox to it. setup.sh CAN
call this automatically now too (its Step 8B) - see setup.sh's own
comments - but only when HDB_ALERT_RECIPIENT_EMAILS is set first, since
that step is skipped, not defaulted, when no email is given.

WHY THE EMAIL ISN'T HARDCODED ANYWHERE IN CODE:
Same reasoning as the AWS account id (see common.py's get_account_id()) -
an email address is personal data, and this is a public GitHub repo. There
is also no reliable way to "just derive it from the AWS account" - the only
account-level email AWS exposes is the root/billing contact, reachable only
via the Account Management API's account:GetContactInformation action
(usually restricted to the root user / account admins, not whatever role
runs this pipeline), and even where accessible it's often a shared ops
inbox, not the individual engineer actually running this - auto-subscribing
that without asking would likely be wrong. So this asks explicitly instead,
either via config.ALERT_RECIPIENT_EMAILS (HDB_ALERT_RECIPIENT_EMAILS env
var) or the --email flag below.

The topic ARN is computed the SAME way common.py's send_alert() builds it
at runtime - live account id via STS + region + topic name - so this script
is guaranteed to subscribe to the exact topic the pipeline actually
publishes to, not a copy-pasted ARN that could drift out of sync.

Usage (run from the hdb_1/ project root):
    # Uses ALERT_RECIPIENT_EMAILS from config.py / HDB_ALERT_RECIPIENT_EMAILS
    python sns_subscription_setup.py --region us-east-1

    # Or subscribe specific email(s) directly, ignoring config:
    python sns_subscription_setup.py --region us-east-1 \\
        --email you@example.com --email teammate@example.com

    # One-command version (setup.sh calls this script for you, Step 8B):
    export HDB_ALERT_RECIPIENT_EMAILS=you@example.com
    bash setup.sh

AFTER RUNNING: AWS immediately sends a confirmation email to each address.
The subscription stays in PendingConfirmation (no alerts delivered) until
someone clicks the confirmation link in that email - this script cannot do
that part for you.
"""

import argparse
import sys
from pathlib import Path

import boto3

# config.py is the pipeline's single source of truth for ALERT_RECIPIENT_EMAILS.
# This script sits at the hdb_1/ project root; config.py/common.py live one
# level down, in pipeline-scripts/05_ETL/.
sys.path.insert(0, str(Path(__file__).resolve().parent / "pipeline-scripts" / "05_ETL"))
from config import ALERT_RECIPIENT_EMAILS, SNS_TOPIC_NAME  # noqa: E402

parser = argparse.ArgumentParser(description="HDB pipeline - subscribe email(s) to the SNS alert topic")
parser.add_argument("--region", required=True, help="AWS region")
parser.add_argument(
    "--email", action="append", default=None,
    help="Email address to subscribe. Repeat for multiple. Defaults to config.ALERT_RECIPIENT_EMAILS "
         "(HDB_ALERT_RECIPIENT_EMAILS env var) if omitted.",
)
parser.add_argument("--topic-name", default=SNS_TOPIC_NAME, help="SNS topic name (must match what setup.sh created)")
args = parser.parse_args()

emails = args.email or ALERT_RECIPIENT_EMAILS
if not emails:
    print(
        "No email address given - pass --email you@example.com (repeatable) "
        "or set HDB_ALERT_RECIPIENT_EMAILS=you@example.com,teammate@example.com first."
    )
    sys.exit(1)

sts = boto3.client("sts", region_name=args.region)
account_id = sts.get_caller_identity()["Account"]
topic_arn = f"arn:aws:sns:{args.region}:{account_id}:{args.topic_name}"

sns = boto3.client("sns", region_name=args.region)

print("============================================================")
print("HDB Pipeline - SNS Email Subscription Setup")
print(f"Topic: {topic_arn}")
print("============================================================")

existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
already_subscribed = {s["Endpoint"] for s in existing if s["Protocol"] == "email"}

for email in emails:
    if email in already_subscribed:
        print(f"  {email} - already subscribed, skipping")
        continue
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"  {email} - subscription requested (check inbox for a confirmation email)")

print("------------------------------------------------------------")
print("IMPORTANT: each address above must click the confirmation link AWS")
print("just emailed it - until then it stays PendingConfirmation and will")
print("NOT receive pipeline alerts. Re-run with the same args any time to")
print("check status; already-confirmed addresses print 'already subscribed'.")
