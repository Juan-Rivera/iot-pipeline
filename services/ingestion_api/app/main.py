import os
import boto3
from time import time
import logging

from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    logger.addHandler(handler)

API_KEY_SECRET_ARN = os.environ.get("API_KEY_SECRET_ARN")
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)


_API_KEY_CACHE = {
    "value": None,
    "expires_at": 0.0,
}
_API_KEY_TTL_SECONDS = 60


def get_api_key_from_secrets_manager() -> str:
    # Get current timestamp
    now = time()
    if _API_KEY_CACHE["value"] and _API_KEY_CACHE["expires_at"] > now:
        return _API_KEY_CACHE["value"]

    if not API_KEY_SECRET_ARN:
        logger.error("API_KEY_SECRET_ARN is not set in environment.")
        raise RuntimeError("API_KEY_SECRET_ARN not configured")

    try:
        secret_response = secrets_client.get_secret_value(SecretId=API_KEY_SECRET_ARN)
        secret_string = secret_response.get("SecretString")
        if not secret_string:
            raise RuntimeError("SecretString is empty or missing")
    except Exception as e:
        logger.exception("Failed to fetch API key from Secrets Manager: %s", e)
        raise RuntimeError("Unable to read API key from Secrets Manager") from e

    _API_KEY_CACHE["value"] = secret_string
    _API_KEY_CACHE["expires_at"] = now + _API_KEY_TTL_SECONDS
    return secret_string


app = FastAPI()
app.debug = True


class Event(BaseModel):
    name: str
    value: int


def is_authorized(x_api_key: str | None = Header(default=None)) -> None:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key.",
        )
    try:
        expected_key = get_api_key_from_secrets_manager()
    except RuntimeError as e:
        logger.error("Auth misconfiguration: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Auth not configured.",
        )

    if x_api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/health")
def health():
    return {"status": 200, "message": "all good!"}


@app.post("/events", dependencies=[Depends(is_authorized)])
def post_events(event: Event):
    return {"status": 200, "event_received": event}
