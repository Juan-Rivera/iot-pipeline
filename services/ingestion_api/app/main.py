from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
app.debug = True


class Event(BaseModel):
    name: str
    value: int


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/health")
def health():
    return {"status": 200, "message": "all good!"}


@app.post("/events")
def post_events(event: Event):
    return {"status": 200, "event_received": event}
