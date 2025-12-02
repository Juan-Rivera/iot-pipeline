import asyncio, json, websockets


class HAWebSocketClient:
    def __init__(self, url, token, on_event):
        self.url = url
        self.token = token
        self.on_event = on_event
        self.msg_id = 1

    async def run_forever(self):
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    await self.authenticate(ws)
                    await self.subscribe_all(ws)
                    await self.listen(ws)
            except Exception as e:
                print("WebSocket error:", e)
                await asyncio.sleep(5)

    async def authenticate(self, ws):
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": self.token}))

    async def subscribe_all(self, ws):
        await ws.send(json.dumps({"id": self.msg_id, "type": "subscribe_events"}))
        self.msg_id += 1

    async def listen(self, ws):
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") == "event":
                evt = msg.get("event")
                if evt:
                    self.on_event(evt)
