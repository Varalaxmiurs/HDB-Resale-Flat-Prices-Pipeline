import os
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ============================================================
# LOGGING SETUP
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "ingestion_copy.log")

logger = logging.getLogger("ingestion_copy")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


# ============================================================
# INGESTION SCRIPT
# ============================================================
def copy_local_source_to_raw():
    """
    Copy files from local folder '00_Source_files' into the target RAW_BUCKET.
    Environment variables must be set by sourcing dev.sh / uat.sh / prd.sh.
    """
    logger.info("=" * 60)
    logger.info("Starting local folder copy to Raw Bucket")
    logger.info("=" * 60)

    source_dir = os.path.join(PROJECT_ROOT, "00_Source_files")

    if not os.path.exists(source_dir):
        logger.error(f"Source folder does not exist: {source_dir}")
        return

    try:
        raw_bucket_name = os.environ["RAW_BUCKET"]   # comes from dev.sh
        region = os.environ.get("REGION", "us-east-1")

        logger.info(f"AWS Region         : {region}")
        logger.info(f"Target S3 Bucket   : {raw_bucket_name}")

        s3_client = boto3.client("s3", region_name=region)

        # Verify bucket accessibility
        s3_client.head_bucket(Bucket=raw_bucket_name)

        # Ensure "raw/data/" prefix exists (simulate folder)
        folder_key = "raw/data/"
        s3_client.put_object(Bucket=raw_bucket_name, Key=folder_key)
        logger.info(f"Ensured folder prefix exists: s3://{raw_bucket_name}/{folder_key}")

    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS initialization error: {e}")
        return

    # Upload files from local source folder
    files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]

    if not files:
        logger.warning(f"No files found in directory: {source_dir}")
        return

    success_count, fail_count = 0, 0

    for file_name in files:
        file_path = os.path.join(source_dir, file_name)
        target_key = f"raw/data/{file_name}"

        try:
            logger.info(f"Uploading '{file_name}' to 's3://{raw_bucket_name}/{target_key}'...")
            s3_client.upload_file(file_path, raw_bucket_name, target_key)
            logger.info(f"Successfully uploaded '{file_name}'")
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to upload '{file_name}': {e}")
            fail_count += 1

    logger.info("=" * 60)
    logger.info(f"Upload Summary: {success_count} succeeded, {fail_count} failed.")
    logger.info("=" * 60)


# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":
    copy_local_source_to_raw()
