import time
import json
import logging

import aws_utils

aws_utils.configure_logging()

logger = logging.getLogger(__name__)

last_seq = None


def main():
    global last_seq
    shard_iterator, shard_id = aws_utils.get_shard_iterator()
    buffer = []
    total_events = 0
    start_time = time.time()
    last_flush_time = time.time()

    logger.info("Consumer started.")

    while True:
        records, shard_iterator = aws_utils.read_records(shard_iterator)
        if records:
            last_seq = records[-1]["SequenceNumber"]
        iterator_age = aws_utils.get_iterator_age()

        batch_start = time.time()

        for r in records:
            try:
                data = json.loads(r["Data"])
                evt = data.get("event", {})
                entity = evt.get("data", {}).get("entity_id")

                if not entity:
                    continue

                event_key = aws_utils.compute_event_key(evt)

                if not aws_utils.register_event(event_key):
                    logger.info(f"Duplicate skipped: {event_key}")
                    continue

                buffer.append(data)
                total_events += 1

            except Exception as e:
                logger.error(f"Record decode error: {e}")

        if len(buffer) == 0 and len(records) > 0 and last_seq:
            aws_utils.save_checkpoint(shard_id, last_seq)

        if _should_flush(buffer, last_flush_time):
            flush_start = time.time()
            aws_utils.write_parquet_and_upload(buffer)
            flush_end = time.time()

            if last_seq:
                aws_utils.save_checkpoint(shard_id, last_seq)

            aws_utils.emit_metrics(
                batch_size=len(buffer),
                batch_latency=flush_end - batch_start,
                flush_latency=flush_end - flush_start,
                iterator_age=iterator_age,
                total_events=total_events,
                start_time=start_time,
            )

            buffer = []
            last_flush_time = time.time()

        time.sleep(0.2)


MAX_BUFFER_SIZE = 1000
MAX_BATCH_SECONDS = 1.0


def _should_flush(buffer, last_flush_time):
    if not buffer:
        return False

    # now = time.time()
    buffer_full = len(buffer) >= MAX_BUFFER_SIZE
    # time_exceeded = (now - last_flush_time) >= MAX_BATCH_SECONDS
    return buffer_full


if __name__ == "__main__":
    main()
