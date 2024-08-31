from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData
from app.core.config import settings

class EventHubClient:
    def __init__(self):
        credential = ClientSecretCredential(
            tenant_id=settings.azure_tenant_id,
            client_id=settings.azure_client_id,
            client_secret=settings.azure_client_secret
        )
        self.producer = EventHubProducerClient(
            fully_qualified_namespace=settings.eventhub_namespace,
            eventhub_name=settings.eventhub_name,
            credential=credential
        )

    async def send_event(self, data):
        async with self.producer:
            event_data_batch = await self.producer.create_batch()
            event_data_batch.add(EventData(data))
            await self.producer.send_batch(event_data_batch)

    async def close(self):
        await self.producer.close()
