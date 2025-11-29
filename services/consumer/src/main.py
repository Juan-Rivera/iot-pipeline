import time
import json
import logging

import aws_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())


def main():
    shard_iterator, shard_id = aws_utils.get_shard_iterator()
    buffer = []

    logger.info("Consumer started.")

    while True:
        records, shard_iterator = aws_utils.read_records(shard_iterator)

        for r in records:
            try:
                data = json.loads(r["Data"])
                evt = data.get("event", {})
                entity = evt.get("data", {}).get("entity_id")

                if not entity:
                    continue

                event_key = aws_utils.compute_event_key(data)

                if not aws_utils.register_event(event_key):
                    logger.info(f"Duplicate skipped: {event_key}")
                    continue

                buffer.append(data)

            except Exception as e:
                logger.error(f"Record decode error: {e}")

        if _should_flush(buffer, records):
            aws_utils.write_parquet_and_upload(buffer)
            if records:
                last_seq = records[-1]["SequenceNumber"]
                aws_utils.save_checkpoint(shard_id, last_seq)
            buffer = []

        time.sleep(0.25)


def _should_flush(buffer, records):
    return len(buffer) >= 250 or (buffer and len(records) == 0)


if __name__ == "__main__":
    main()
