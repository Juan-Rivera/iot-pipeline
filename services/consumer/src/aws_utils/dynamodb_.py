import time
import hashlib
import json
import logging

from clients import dynamodb, IDEMPOTENCY_TABLE, DEDUP_TTL_DAYS, CHECKPOINT_TABLE

logger = logging.getLogger(__name__)


def compute_event_key(event: dict) -> str:
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def register_event(event_key: str) -> bool:
    ttl = int(time.time()) + DEDUP_TTL_DAYS * 86400

    try:
        dynamodb.put_item(
            TableName=IDEMPOTENCY_TABLE,
            Item={
                "event_key": {"S": event_key},
                "expire_at": {"N": str(ttl)},
            },
            ConditionExpression="attribute_not_exists(event_key)",
        )
        return True

    except dynamodb.exceptions.ConditionalCheckFailedException:
        return False


def load_checkpoint(shard_id: str):
    """
    Return stored sequence number or None if first run.
    """
    try:
        resp = dynamodb.get_item(
            TableName=CHECKPOINT_TABLE, Key={"shard_id": {"S": shard_id}}
        )
        item = resp.get("Item")
        if not item:
            return None
        return item["sequence_number"]["S"]
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        return None


def save_checkpoint(shard_id: str, seq: str):
    """
    Save the latest fully processed sequence number.
    Safe because consumer writes only after S3 upload.
    """
    try:
        dynamodb.put_item(
            TableName=CHECKPOINT_TABLE,
            Item={
                "shard_id": {"S": shard_id},
                "sequence_number": {"S": seq},
            },
        )
        logger.info(f"Checkpoint updated: shard={shard_id} seq={seq}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
