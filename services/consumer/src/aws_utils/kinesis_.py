import logging
from clients import kinesis, KINESIS_STREAM
from .dynamodb_ import load_checkpoint

logger = logging.getLogger(__name__)

_iterator_age_seconds = 0.0


def resolve_shard():
    stream_info = kinesis.describe_stream(StreamName=KINESIS_STREAM)
    shard_id = stream_info["StreamDescription"]["Shards"][0]["ShardId"]
    return shard_id


def get_shard_iterator():
    shard_id = resolve_shard()
    checkpoint = load_checkpoint(shard_id)

    if checkpoint:
        logger.info(f"Resuming from checkpoint seq={checkpoint}")
        iterator_type = "AFTER_SEQUENCE_NUMBER"
        args = {
            "StreamName": KINESIS_STREAM,
            "ShardId": shard_id,
            "ShardIteratorType": iterator_type,
            "StartingSequenceNumber": checkpoint,
        }
    else:
        logger.info("No checkpoint found — starting from TRIM_HORIZON")
        args = {
            "StreamName": KINESIS_STREAM,
            "ShardId": shard_id,
            "ShardIteratorType": "TRIM_HORIZON",
        }

    return kinesis.get_shard_iterator(**args)["ShardIterator"], shard_id


def read_records(shard_iterator: str, limit=1000):
    global _iterator_age_seconds

    resp = kinesis.get_records(
        ShardIterator=shard_iterator,
        Limit=limit,
    )

    millis_behind = resp.get("MillisBehindLatest", 0)
    _iterator_age_seconds = millis_behind / 1000.0

    return resp.get("Records", []), resp.get("NextShardIterator")


def get_iterator_age():
    return _iterator_age_seconds
