import boto3
import os
import json
import traceback
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ecs = boto3.client("ecs")
secrets = boto3.client("secretsmanager")

CLUSTER_ARN = os.environ["CLUSTER_ARN"]
SERVICE_ARN = os.environ["SERVICE_ARN"]
API_KEY_SECRET_ARN = os.environ["API_KEY_SECRET_ARN"]


def lambda_handler(event, context):
    logger.info("=== STOP BACKEND LAMBDA INVOKED ===")

    try:
        supplied_key = event.get("headers", {}).get("x-api-key")
        if not supplied_key:
            return {"status_code": 401, "body": "Missing x-api-key header"}

        real_secret = secrets.get_secret_value(SecretId=API_KEY_SECRET_ARN)[
            "SecretString"
        ]
        if supplied_key != real_secret:
            logger.warning("Invalid API Key attempt")
            return {"status_code": 403, "body": "Forbidden"}

        tasks = ecs.list_tasks(
            cluster=CLUSTER_ARN, serviceName=SERVICE_ARN, desiredStatus="RUNNING"
        )["taskArns"]

        logger.info(f"Running Tasks Found: {tasks}")

        stopped = []

        for task_arn in tasks:
            logger.info(f"Stopping task: {task_arn}")
            resp = ecs.stop_task(
                cluster=CLUSTER_ARN,
                task=task_arn,
                reason="Manual stop triggered by control API",
            )
            stopped.append(resp)

        logger.info("Setting desiredCount=0 to fully stop backend")
        ecs.update_service(
            cluster=CLUSTER_ARN,
            service=SERVICE_ARN,
            desiredCount=0,
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "backend_stopped", "stopped_tasks": tasks}),
        }

    except Exception as e:
        logger.error("ERROR stopping backend:")
        logger.error(traceback.format_exc())
        logger.error(str(e))
        return {
            "statusCode": 500,
            "body": "Check logs",
        }
