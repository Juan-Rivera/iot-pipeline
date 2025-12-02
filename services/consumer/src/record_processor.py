import time
import json
import logging
import aws_utils
from amazon_kclpy.v3 import processor

logger = logging.getLogger(__name__)

MAX_BUFFER_SIZE = 1000000
MAX_BATCH_SECONDS = 300.0


class RecordProcessor(processor.RecordProcessorBase):
    def __init__(self):
        self._spiller = aws_utils.ParquetSpiller(batch_size_threshold=1000)
        self._last_flush_time = time.time()
        self._total_events = 0
        self._start_time = time.time()
        self._shard_id = None
        self._items_since_last_flush = 0

    def initialize(self, initialization_input):
        self._shard_id = initialization_input.shard_id
        logger.info(f"Initializing RecordProcessor for shard: {self._shard_id}")

    def process_records(self, process_records_input):
        records = process_records_input.records
        millis_behind_latest = process_records_input.millis_behind_latest
        checkpointer = process_records_input.checkpointer

        parsed_records = []

        for r in records:
            try:
                data = json.loads(r.binary_data)
                evt = data.get("event", {})
                entity = evt.get("data", {}).get("entity_id")

                if not entity:
                    continue

                parsed_records.append({"data": data})

            except Exception as e:
                logger.error(
                    f"Record decode error: {e}. Data type: {type(r.data)}. Data: {r.data!r}"
                )

        for item in parsed_records:
            self._spiller.add_record(item)

        self._total_events += len(parsed_records)
        self._items_since_last_flush += len(parsed_records)

        if self._should_flush():
            self._flush_buffer(millis_behind_latest / 1000.0)
            try:
                checkpointer.checkpoint()
            except Exception as e:
                logger.error(f"Checkpoint failed: {e}")

    def lease_lost(self, lease_lost_input):
        logger.info("Lease lost")

    def shard_ended(self, shard_ended_input):
        logger.info("Shard ended")
        self._flush_buffer(0.0)
        try:
            shard_ended_input.checkpointer.checkpoint()
        except Exception as e:
            logger.error(f"Checkpoint failed at shard end: {e}")

    def shutdown_requested(self, shutdown_requested_input):
        logger.info("Shutdown requested")
        self._flush_buffer(0.0)
        try:
            shutdown_requested_input.checkpointer.checkpoint()
        except Exception as e:
            logger.error(f"Checkpoint failed at shutdown: {e}")

    def _should_flush(self):
        if self._items_since_last_flush == 0:
            return False

        count_exceeded = self._items_since_last_flush >= MAX_BUFFER_SIZE
        # not implementing time based flushing to stress-test buffer flushing
        # time_exceeded = (time.time() - self._last_flush_time) >= MAX_BATCH_SECONDS

        return count_exceeded

    def _flush_buffer(self, iterator_age_seconds):
        if self._items_since_last_flush == 0:
            return

        flush_start = time.time()

        self._spiller.close_and_upload()

        flush_end = time.time()

        aws_utils.emit_metrics(
            batch_size=self._items_since_last_flush,
            batch_latency=flush_end - self._last_flush_time,
            flush_latency=flush_end - flush_start,
            iterator_age=iterator_age_seconds,
            total_events=self._total_events,
            start_time=self._start_time,
        )

        self._items_since_last_flush = 0
        self._last_flush_time = time.time()
