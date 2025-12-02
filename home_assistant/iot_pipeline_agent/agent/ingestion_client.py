import asyncio, aiohttp, json, time
from collections import deque


class IngestionClient:
    def __init__(self, backend_url, api_key, max_batch=1000, flush_interval=0.3):
        self.backend_url = backend_url
        self.api_key = api_key
        self.queue = deque()
        self.max_batch = max_batch
        self.flush_interval = flush_interval
        self.backoff = 0

    def enqueue_event(self, evt):
        evt["received_at"] = time.time()
        self.queue.append(evt)

    async def run_flush_loop(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush()

    async def flush(self):
        if not self.queue or self.backoff > 0:
            if self.backoff > 0:
                self.backoff -= 1
            return

        batch = []
        while self.queue and len(batch) < self.max_batch:
            batch.append(self.queue.popleft())

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    self.backend_url,
                    data=json.dumps({"events": batch}),
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    timeout=5,
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"HTTP {resp.status}")
            except Exception as e:
                print("Flush failed:", e)
                self.backoff = min(int(self.backoff * 2) + 1, 20)
                for evt in reversed(batch):
                    self.queue.appendleft(evt)
