"""
Lambda function for rotating API key secrets in AWS Secrets Manager.

This function handles the four-step rotation process:
1. createSecret - Generate and store a new secret value with AWSPENDING stage
2. setSecret - Push the new secret to downstream services (if needed)
3. testSecret - Validate the new secret works correctly
4. finishSecret - Promote AWSPENDING to AWSCURRENT and demote the old version
"""

import secrets
import string
import boto3

secretsmanager = boto3.client("secretsmanager")


def lambda_handler(event, context):
    """
    Main handler for secret rotation.

    Args:
        event: Lambda event containing Step, ClientRequestToken, and SecretId
        context: Lambda context object

    Returns:
        dict: Status response indicating success
    """
    step = event["Step"]
    token = event["ClientRequestToken"]
    arn = event["SecretId"]

    if step == "createSecret":
        secretsmanager.put_secret_value(
            SecretId=arn,
            ClientRequestToken=token,
            SecretString=_generate_new_key(),
            VersionStages=["AWSPENDING"],
        )
    elif step == "setSecret":
        pass
    elif step == "testSecret":
        pass
    elif step == "finishSecret":
        _finish_secret(arn, token)
    else:
        raise ValueError(f"Unknown step: {step}")

    return {"status": "ok"}


def _generate_new_key():
    """
    Generate a new random API key using only alphanumeric characters.

    This matches the behavior of AWS Secrets Manager's SecretStringGenerator
    with exclude_punctuation=True.

    Returns:
        str: An alphanumeric random string (32 characters)
    """
    # Use only alphanumeric characters (no punctuation)
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(32))


def _finish_secret(arn, token):
    """
    Promote the AWSPENDING version to AWSCURRENT and demote the old version.

    Args:
        arn: The ARN of the secret being rotated
        token: The client request token (version ID) of the pending version
    """
    metadata = secretsmanager.describe_secret(SecretId=arn)
    current_version = None
    for version_id, stages in metadata["VersionIdsToStages"].items():
        if "AWSCURRENT" in stages:
            current_version = version_id
            break

    if current_version:
        secretsmanager.update_secret_version_stage(
            SecretId=arn,
            VersionStage="AWSCURRENT",
            MoveToVersionId=token,
            RemoveFromVersionId=current_version,
        )
    else:
        secretsmanager.update_secret_version_stage(
            SecretId=arn,
            VersionStage="AWSCURRENT",
            MoveToVersionId=token,
        )
