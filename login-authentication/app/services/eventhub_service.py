from infra.eventhub.client import EventHubClient

class EventHubService:
    def __init__(self):
        self.client = EventHubClient()

    async def publish_event(self, data):
        await self.client.send_event(data)

    async def shutdown(self):
        await self.client.close()
