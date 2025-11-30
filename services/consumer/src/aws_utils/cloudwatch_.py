import time
import json
import logging

_metrics_logger = logging.getLogger("metrics")
_metrics_logger.setLevel(logging.INFO)
if not _metrics_logger.handlers:
    _metrics_logger.addHandler(logging.StreamHandler())


def emit_metrics(
    batch_size: int,
    batch_latency: float,
    flush_latency: float,
    iterator_age: float,
    total_events: int,
    start_time: float,
    service: str = "consumer",
):
    """
    Emit CloudWatch Embedded Metric Format (EMF) logs.
    These logs are automatically turned into CloudWatch metrics by AWS.

    Parameters
    ----------
    batch_size : int
        Number of events processed in the batch.
    batch_latency : float
        Full batch cycle time (read → process → flush).
    flush_latency : float
        Flush time (write_parquet_and_upload only).
    iterator_age : float
        Lag in seconds since newest Kinesis record.
    total_events : int
        Total processed events since consumer startup.
    start_time : float
        Timestamp when the service started (for processing_rate).
    """

    now = time.time()
    elapsed = now - start_time

    metric = {
        "_aws": {
            "Timestamp": int(now * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "IoTIngestionPipeline",
                    "Dimensions": [["Service"]],
                    "Metrics": [
                        {"Name": "ingestion_rate", "Unit": "Count/Second"},
                        {"Name": "processing_rate", "Unit": "Count/Second"},
                        {"Name": "batch_latency_seconds", "Unit": "Seconds"},
                        {"Name": "flush_latency_seconds", "Unit": "Seconds"},
                        {"Name": "iterator_age_seconds", "Unit": "Seconds"},
                    ],
                }
            ],
        },
        "Service": service,
        "ingestion_rate": batch_size / batch_latency if batch_latency > 0 else 0,
        "processing_rate": total_events / elapsed if elapsed > 0 else 0,
        "batch_latency_seconds": batch_latency,
        "flush_latency_seconds": flush_latency,
        "iterator_age_seconds": iterator_age,
    }

    _metrics_logger.info(json.dumps(metric))
