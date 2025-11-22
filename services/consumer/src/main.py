import boto3
import time
import json
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

KINESIS_STREAM = os.environ["KINESIS_STREAM"]
S3_BUCKET = os.environ["S3_BUCKET"]

kinesis = boto3.client("kinesis")
s3 = boto3.client("s3")

SHARD_ITER_TYPE = "TRIM_HORIZON"


def get_shard_iterator():
    stream_info = kinesis.describe_stream(StreamName=KINESIS_STREAM)
    shard_id = stream_info["StreamDescription"]["Shards"][0]["ShardId"]

    iterator = kinesis.get_shard_iterator(
        StreamName=KINESIS_STREAM,
        ShardId=shard_id,
        ShardIteratorType=SHARD_ITER_TYPE,
    )["ShardIterator"]

    return iterator


def write_parquet_and_upload(records):
    if not records:
        return

    table = pa.Table.from_pylist(records)

    filename = f"raw/{int(time.time())}-{uuid.uuid4().hex}.parquet"
    local_path = f"/tmp/{uuid.uuid4().hex}.parquet"

    pq.write_table(table, local_path)

    s3.upload_file(local_path, S3_BUCKET, filename)

    logger.info(f"Uploaded {len(records)} records → s3://{S3_BUCKET}/{filename}")


def main():
    shard_iterator = get_shard_iterator()
    buffer = []

    logger.info("Starting consumer loop...")

    while True:
        resp = kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=100,
        )

        shard_iterator = resp["NextShardIterator"]
        records = resp.get("Records", [])

        for r in records:
            try:
                data = json.loads(r["Data"])
                buffer.append(data)
            except Exception as e:
                logger.info(f"Bad record: {e}")

        if len(buffer) >= 100 or (buffer and len(records) == 0):
            write_parquet_and_upload(buffer)
            buffer = []

        if not records:
            time.sleep(2)
        else:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
