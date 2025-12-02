import json
import os
import time
import boto3
import logging

import aws_utils

aws_utils.configure_logging()

logger = logging.getLogger(__name__)

sm = boto3.client("secretsmanager")
SECRET_ARN = os.environ["API_KEY_SECRET_ARN"]


def get_api_key():
    resp = sm.get_secret_value(SecretId=SECRET_ARN)
    return resp["SecretString"]


def _is_authorized(func):
    def wrapper(event, context, *args, **kwargs):
        headers = event.get("headers", {}) or {}
        auth = headers.get("authorization") or headers.get("Authorization")
        start_time = time.time()

        if not auth or not auth.startswith("Bearer "):
            request_latency = time.time() - start_time
            aws_utils.emit_metrics(
                events_received=0,
                events_ingested=0,
                events_ignored=0,
                kinesis_failed=0,
                request_latency=request_latency,
                auth_failed=True,
            )
            return {"statusCode": 401, "body": "Unauthorized"}

        incoming_token = auth.replace("Bearer ", "").strip()
        expected_token = get_api_key()

        if incoming_token != expected_token:
            request_latency = time.time() - start_time
            aws_utils.emit_metrics(
                events_received=0,
                events_ingested=0,
                events_ignored=0,
                kinesis_failed=0,
                request_latency=request_latency,
                auth_failed=True,
            )
            return {"statusCode": 401, "body": "Unauthorized"}

        return func(event, context, *args, start_time=start_time, **kwargs)

    return wrapper


@_is_authorized
def lambda_handler(event, context, start_time=None):
    start = start_time if start_time is not None else time.time()

    try:
        body = json.loads(event.get("body", "{}"))
    except Exception:
        request_latency = time.time() - start
        aws_utils.emit_metrics(
            events_received=0,
            events_ingested=0,
            events_ignored=0,
            request_latency=request_latency,
        )
        return {"statusCode": 400, "body": "Invalid JSON body"}

    incoming_events = body.get("events", [])
    events_received = len(incoming_events)
    valid_records = []
    events_ignored = 0

    for ev in incoming_events:
        entity_id = ev.get("data", {}).get("entity_id")
        if not entity_id:
            events_ignored += 1
            continue

        envelope = {
            "source": "homeassistant",
            "received_at": time.time(),
            "event": ev,
        }

        valid_records.append(
            {
                "Data": json.dumps(envelope),
                "PartitionKey": entity_id,
            }
        )

    if not valid_records:
        request_latency = time.time() - start

        aws_utils.emit_metrics(
            events_received=events_received,
            events_ingested=0,
            events_ignored=events_ignored,
            kinesis_failed=0,
            request_latency=request_latency,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "events_ingested": 0,
                    "events_ignored": events_ignored,
                }
            ),
        }

    response = aws_utils.push_to_kinesis(valid_records)

    failed = response.get("FailedRecordCount", 0)
    ingested_count = len(valid_records) - failed
    request_latency = time.time() - start

    aws_utils.emit_metrics(
        events_received=events_received,
        events_ingested=ingested_count,
        events_ignored=events_ignored,
        request_latency=request_latency,
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "events_ingested": len(valid_records),
                "events_ignored": events_ignored,
                "partial_failures": failed,
            }
        ),
    }
