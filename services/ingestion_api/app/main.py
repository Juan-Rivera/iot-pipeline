import logging

from fastapi import Depends, FastAPI
from pydantic import BaseModel

from .auth import is_authorized

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    logger.addHandler(handler)

app = FastAPI()


class Event(BaseModel):
    name: str
    value: int


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/health")
def health():
    return {"status": 200, "message": "all good!"}


@app.post("/events", dependencies=[Depends(is_authorized)])
def post_events(event: Event):
    return {"status": 200, "event_received": event}
