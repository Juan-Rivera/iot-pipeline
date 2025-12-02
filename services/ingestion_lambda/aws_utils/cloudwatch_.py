import time
import json
import logging

_metrics_logger = logging.getLogger("ingestion-metrics")
_metrics_logger.setLevel(logging.INFO)
if not _metrics_logger.handlers:
    _metrics_logger.addHandler(logging.StreamHandler())


def emit_metrics(
    events_received: int = None,
    events_ingested: int = None,
    events_ignored: int = None,
    kinesis_failed: int = None,
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
    metric_config = {
        "events_received": {"value": events_received, "Unit": "Count"},
        "events_ingested": {"value": events_ingested, "Unit": "Count"},
        "events_ignored": {"value": events_ignored, "Unit": "Count"},
        "kinesis_failed_records": {"value": kinesis_failed, "Unit": "Count"},
        "request_latency_seconds": {"value": request_latency, "Unit": "Seconds"},
        "kinesis_write_latency_seconds": {
            "value": kinesis_write_latency,
            "Unit": "Seconds",
        },
        "auth_failures": {"value": 1 if auth_failed else 0, "Unit": "Count"},
    }
    active_metrics = {
        name: config
        for name, config in metric_config.items()
        if config["value"] is not None
    }

    metric = {
        "_aws": {
            "Timestamp": now,
            "CloudWatchMetrics": [
                {
                    "Namespace": "IoTIngestionPipeline",
                    "Dimensions": [["Service"]],
                    "Metrics": [
                        {"Name": name, "Unit": config["Unit"]}
                        for name, config in active_metrics.items()
                    ],
                }
            ],
        },
        "Service": service,
        **{name: config["value"] for name, config in active_metrics.items()},
    }

    _metrics_logger.info(json.dumps(metric))
