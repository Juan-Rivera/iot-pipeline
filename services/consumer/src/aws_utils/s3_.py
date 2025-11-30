import logging
import uuid
import os
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq

from clients import s3, S3_BUCKET

logger = logging.getLogger(__name__)


def _upload_group(entity_id, group_records):
    try:
        table = pa.Table.from_pylist(group_records)

        now = datetime.now(timezone.utc)
        year = now.year
        month = f"{now.month:02d}"
        day = f"{now.day:02d}"
        timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        filename = (
            f"raw/entity_id={entity_id}/"
            f"year={year}/month={month}/day={day}/"
            f"{timestamp}-{uuid.uuid4().hex}.parquet"
        )

        local_path = f"/tmp/{uuid.uuid4().hex[:8]}.parquet"
        pq.write_table(table, local_path, compression="snappy")

        s3.upload_file(local_path, S3_BUCKET, filename)

        os.remove(local_path)

        logger.info(
            f"Uploaded {len(group_records)} records → s3://{S3_BUCKET}/{filename}"
        )

        return filename
    except Exception as e:
        logger.error(f"Failed to upload group (entity_id={entity_id}): {e}")
        raise


def write_parquet_and_upload(records):
    if not records:
        return

    groups = defaultdict(list)

    for record in records:
        entity_id = record["event"]["data"]["entity_id"]
        groups[entity_id].append(record)

    logger.info(
        f"Grouped {len(records)} records into {len(groups)} groups by entity_id"
    )

    uploaded_files = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}

        for entity_id, group_records in groups.items():
            future = executor.submit(_upload_group, entity_id, group_records)
            futures[future] = entity_id

        for future in as_completed(futures):
            entity_id = futures[future]
            try:
                filename = future.result()
                uploaded_files.append(filename)
            except Exception as e:
                logger.error(f"Upload failed for entity_id={entity_id}: {e}")

    logger.info(f"Successfully uploaded {len(uploaded_files)} files to S3")
    return uploaded_files
