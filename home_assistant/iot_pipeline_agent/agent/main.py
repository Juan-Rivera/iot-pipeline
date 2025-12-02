import asyncio, os
from websocket_client import HAWebSocketClient
from ingestion_client import IngestionClient


async def main():
    ingestion = IngestionClient(
        backend_url=os.environ["BACKEND_URL"],
        api_key=os.environ["API_KEY"],
        max_batch=1000,
        flush_interval=0.3,
    )

    ha_ws = HAWebSocketClient(
        url="ws://homeassistant.local:8123/api/websocket",
        token=os.environ["HA_TOKEN"],
        on_event=ingestion.enqueue_event,
    )

    await asyncio.gather(ha_ws.run_forever(), ingestion.run_flush_loop())


if __name__ == "__main__":
    asyncio.run(main())
