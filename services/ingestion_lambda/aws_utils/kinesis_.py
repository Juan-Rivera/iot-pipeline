import boto3
import logging
import time
import os

from typing import Dict

from .cloudwatch_ import emit_metrics

logger = logging.getLogger(__name__)

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["KINESIS_STREAM"]


def push_to_kinesis(all_records) -> Dict[str, int]:
    BATCH_SIZE = 500
    MAX_RETRIES = 3
    output = {"FailedRecordCount": 0}

    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i : i + BATCH_SIZE]

        current_batch = batch
        retries = 0

        while True:
            try:
                kinesis_start = time.time()
                response = kinesis.put_records(
                    StreamName=STREAM_NAME, Records=current_batch
                )
                kinesis_end = time.time()

                failed_count = response.get("FailedRecordCount", 0)

                emit_metrics(
                    kinesis_write_latency=kinesis_end - kinesis_start,
                    kinesis_failed=failed_count,
                )

                if failed_count == 0:
                    break

                if retries >= MAX_RETRIES:
                    logger.warning(
                        f"Failed to ingest {failed_count} records after {MAX_RETRIES} retries"
                    )
                    output["FailedRecordCount"] += failed_count
                    break

                next_batch = []
                for idx, res in enumerate(response["Records"]):
                    if "ErrorCode" in res:
                        next_batch.append(current_batch[idx])

                current_batch = next_batch
                retries += 1
                time.sleep(0.1 * (2 ** (retries - 1)))

            except Exception as e:
                logger.error(f"Kinesis Batch Exception: {e}")
                output["FailedRecordCount"] += len(current_batch)
                break

    return output
