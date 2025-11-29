import logging
import uuid
import time

import pyarrow as pa
import pyarrow.parquet as pq

from clients import s3, S3_BUCKET

logger = logging.getLogger(__name__)


def write_parquet_and_upload(records):
    if not records:
        return

    table = pa.Table.from_pylist(records)

    entity_id = records[0]["event"]["data"]["entity_id"]
    timestamp = int(time.time())

    filename = f"raw/entity_id={entity_id}/" f"{timestamp}-{uuid.uuid4().hex}.parquet"

    local_path = f"/tmp/{uuid.uuid4().hex}.parquet"

    pq.write_table(table, local_path)
    s3.upload_file(local_path, S3_BUCKET, filename)

    logger.info(f"Uploaded {len(records)} records → s3://{S3_BUCKET}/{filename}")
