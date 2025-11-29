import logging
from clients import kinesis, KINESIS_STREAM
from .dynamodb_ import load_checkpoint

logger = logging.getLogger(__name__)


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


def read_records(shard_iterator: str, limit=100):
    resp = kinesis.get_records(
        ShardIterator=shard_iterator,
        Limit=limit,
    )
    return resp.get("Records", []), resp.get("NextShardIterator")
