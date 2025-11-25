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
    logger.info("=== START BACKEND LAMBDA INVOKED ===")

    try:
        supplied_key = event.get("headers", {}).get("x-api-key")
        if not supplied_key:
            return {"statusCode": 401, "body": "Missing x-api-key header"}

        real_secret = secrets.get_secret_value(SecretId=API_KEY_SECRET_ARN)[
            "SecretString"
        ]
        if supplied_key != real_secret:
            logger.warning("Invalid API Key attempt")
            return {"status_code": 403, "body": "Forbidden"}

        response = ecs.update_service(
            cluster=CLUSTER_ARN,
            service=SERVICE_ARN,
            desiredCount=1,
        )
        logger.info(f"ECS UpdateService Response: {json.dumps(response, default=str)}")

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "starting_backend"}),
        }

    except Exception as e:
        logger.error("ERROR while starting backend:")
        logger.error(traceback.format_exc())
        logger.error(str(e))
        return {
            "statusCode": 500,
            "body": "Check logs",
        }
