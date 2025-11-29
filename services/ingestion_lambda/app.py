import json
import os
import time
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["KINESIS_STREAM"]

sm = boto3.client("secretsmanager")
SECRET_ARN = os.environ["API_KEY_SECRET_ARN"]


def get_api_key():
    resp = sm.get_secret_value(SecretId=SECRET_ARN)
    return resp["SecretString"]


def _is_authorized(func):
    def wrapper(event, context, *args, **kwargs):
        headers = event.get("headers", {})
        auth = headers.get("authorization") or headers.get("Authorization")

        if not auth or not auth.startswith("Bearer "):
            return {"statusCode": 401, "body": "Unauthorized"}

        incoming_token = auth.replace("Bearer ", "").strip()
        current_token = get_api_key()

        if incoming_token != current_token:
            return {"statusCode": 401, "body": "Unauthorized"}

        return func(event, context, *args, **kwargs)

    return wrapper


@_is_authorized
def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
    except Exception:
        return {"statusCode": 400, "body": "Invalid JSON body"}

    valid_records = []
    ignored = 0

    for ev in body.get("events", []):
        entity_id = ev.get("data", {}).get("entity_id")

        if not entity_id:
            ignored += 1
            continue

        envelope = {
            "source": "homeassistant",
            "received_at": time.time(),
            "event": ev,
        }

        partition_key = entity_id or ev.get("event_type") or "unknown"

        valid_records.append(
            {
                "Data": json.dumps(envelope),
                "PartitionKey": partition_key,
            }
        )

    if not valid_records:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "events_ingested": 0,
                    "events_ignored": ignored,
                }
            ),
        }

    response = kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=valid_records,
    )

    failed = response.get("FailedRecordCount", 0)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "events_ingested": len(valid_records),
                "events_ignored": ignored,
                "partial_failures": failed,
            }
        ),
    }
