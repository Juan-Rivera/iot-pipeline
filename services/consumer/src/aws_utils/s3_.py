import logging
import uuid
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from clients import s3, S3_BUCKET

logger = logging.getLogger(__name__)


class ParquetSpiller:
    def __init__(self, batch_size_threshold=1000):
        self._buffer = []
        self._batch_size_threshold = batch_size_threshold
        self._writer = None
        self._current_file = f"/tmp/{uuid.uuid4().hex}.parquet"
        self._record_count = 0

    def add_record(self, record):
        self._buffer.append(record)

        if len(self._buffer) >= self._batch_size_threshold:
            self._flush_buffer_to_disk()

    def _flush_buffer_to_disk(self):
        if not self._buffer:
            return

        try:
            table = pa.Table.from_pylist(self._buffer)

            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    self._current_file, table.schema, compression="snappy"
                )

            self._writer.write_table(table)
            self._record_count += len(self._buffer)
            self._buffer = []

        except Exception as e:
            logger.error(f"Failed to spill to disk: {e}")
            raise

    def close_and_upload(self):
        self._flush_buffer_to_disk()

        if self._writer:
            self._writer.close()

        if self._record_count == 0:
            if os.path.exists(self._current_file):
                os.remove(self._current_file)
            return None

        try:
            now = datetime.now(timezone.utc)
            year = now.year
            month = f"{now.month:02d}"
            day = f"{now.day:02d}"
            timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

            filename = (
                f"raw/year={year}/month={month}/day={day}/"
                f"{timestamp}-{uuid.uuid4().hex}.parquet"
            )

            s3.upload_file(self._current_file, S3_BUCKET, filename)
            logger.info(
                f"Uploaded {self._record_count} records → s3://{S3_BUCKET}/{filename}"
            )

            return filename
        finally:
            if os.path.exists(self._current_file):
                os.remove(self._current_file)

            self._current_file = f"/tmp/{uuid.uuid4().hex}.parquet"
            self._writer = None
            self._record_count = 0
