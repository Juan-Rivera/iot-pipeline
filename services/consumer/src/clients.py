import os
import boto3

KINESIS_STREAM = os.environ["KINESIS_STREAM"]
S3_BUCKET = os.environ["S3_BUCKET"]
IDEMPOTENCY_TABLE = os.environ["IDEMPOTENCY_TABLE"]
CHECKPOINT_TABLE = os.environ["CHECKPOINT_TABLE"]
DEDUP_TTL_DAYS = int(os.environ.get("DEDUP_TTL_DAYS", "30"))

# AWS clients
kinesis = boto3.client("kinesis")
s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")
