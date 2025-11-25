import boto3
import os
import json
import traceback
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cw = boto3.client("cloudwatch")
ecs = boto3.client("ecs")

CLUSTER_ARN = os.environ["CLUSTER_ARN"]
SERVICE_ARN = os.environ["SERVICE_ARN"]
LB_FULL_NAME = os.environ["LB_FULL_NAME"]
IDLE_HOURS = int(os.environ.get("IDLE_HOURS", "3"))


def lambda_handler(event, context):
    logger.info("=== IDLE REAPER INVOKED ===")

    try:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=IDLE_HOURS)

        logger.info(f"Checking RequestCount from {start} → {now}")

        metrics = cw.get_metric_statistics(
            Namespace="AWS/ApplicationELB",
            MetricName="RequestCount",
            Dimensions=[{"Name": "LoadBalancer", "Value": LB_FULL_NAME}],
            StartTime=start,
            EndTime=now,
            Period=300,
            Statistics=["Sum"],
        )

        logger.info(f"Retrieved Metrics: {json.dumps(metrics, default=str)}")

        total = sum(dp["Sum"] for dp in metrics.get("Datapoints", []))
        logger.info(f"Total RequestCount: {total}")

        if total > 0:
            logger.info("Backend is active — NOT scaling down.")
            return {"status": "active", "request_count": total}

        logger.info("Backend idle — scaling down to 0")

        ecs_response = ecs.update_service(
            cluster=CLUSTER_ARN,
            service=SERVICE_ARN,
            desiredCount=0,
        )

        logger.info(
            f"ECS UpdateService Response: {json.dumps(ecs_response, default=str)}"
        )

        return {"status": "scaled_to_zero", "request_count": 0}

    except Exception as e:
        logger.error("ERROR in Idle Reaper:")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "details": str(e),
        }
