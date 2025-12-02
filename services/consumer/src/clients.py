import os
import boto3

S3_BUCKET = os.environ["S3_BUCKET"]
DEDUP_TTL_DAYS = int(os.environ.get("DEDUP_TTL_DAYS", "30"))

# AWS clients
s3 = boto3.client("s3")
