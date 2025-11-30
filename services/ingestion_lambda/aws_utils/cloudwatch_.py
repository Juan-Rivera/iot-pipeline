import time
import json
import logging

_metrics_logger = logging.getLogger("ingestion-metrics")
_metrics_logger.setLevel(logging.INFO)
if not _metrics_logger.handlers:
    _metrics_logger.addHandler(logging.StreamHandler())


def emit_metrics(
    events_received: int,
    events_ingested: int,
    events_ignored: int,
    kinesis_failed: int,
    request_latency: float = None,
    kinesis_write_latency: float = None,
    auth_failed: bool = False,
    service: str = "ingestion_lambda",
):
    """
    Emit CloudWatch Embedded Metric Format (EMF) logs for ingestion Lambda.

    These logs automatically become CloudWatch metrics.

    Parameters
    ----------
    events_received : int
        Total events received in the request payload.
    events_ingested : int
        Number of events successfully written to Kinesis.
    events_ignored : int
        Events ignored due to missing entity_id or malformed structure.
    kinesis_failed : int
        Number of records Kinesis rejected (partial failures).
    request_latency : float
        Full Lambda request duration (seconds).
    kinesis_write_latency : float
        Duration of Kinesis put_records call (0 when no valid records).
    auth_failed : bool
        Whether auth failed for this request.
    service : str
        Metric dimension name (defaults to ingestion_lambda).
    """

    now = int(time.time() * 1000)

    metric = {
        "_aws": {
            "Timestamp": now,
            "CloudWatchMetrics": [
                {
                    "Namespace": "IoTIngestionPipeline",
                    "Dimensions": [["Service"]],
                    "Metrics": [
                        {"Name": "events_received", "Unit": "Count"},
                        {"Name": "events_ingested", "Unit": "Count"},
                        {"Name": "events_ignored", "Unit": "Count"},
                        {"Name": "kinesis_failed_records", "Unit": "Count"},
                        {"Name": "request_latency_seconds", "Unit": "Seconds"},
                        {"Name": "kinesis_write_latency_seconds", "Unit": "Seconds"},
                        {"Name": "auth_failures", "Unit": "Count"},
                    ],
                }
            ],
        },
        "Service": service,
        "events_received": events_received,
        "events_ingested": events_ingested,
        "events_ignored": events_ignored,
        "kinesis_failed_records": kinesis_failed,
        **(
            {"request_latency_seconds": request_latency}
            if request_latency is not None
            else {}
        ),
        **(
            {"kinesis_write_latency_seconds": kinesis_write_latency}
            if kinesis_write_latency is not None
            else {}
        ),
        "auth_failures": 1 if auth_failed else 0,
    }

    _metrics_logger.info(json.dumps(metric))
