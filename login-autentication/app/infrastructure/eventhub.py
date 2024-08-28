from app.domain.interfaces import EventHubInterface

class EventHub(EventHubInterface):
    async def publish_event(self, event: dict):
        # Implementação para enviar dados ao Event Hub
        pass