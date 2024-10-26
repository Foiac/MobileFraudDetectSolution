from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    azure_tenant_id: str = "dc25df03-ffa5-4111-b188-46fe6cd26a3a" #os.getenv('AZURE_TENANT_ID')
    azure_client_id: str = "7060731f-a210-43a3-8217-e6741e224f03" #os.getenv('AZURE_CLIENT_ID')
    azure_client_secret: str = "91x8Q~UMwd8-OacWvtFOhyluo3LfukC~QgQREdqT" #os.getenv('AZURE_CLIENT_SECRET')
    eventhub_namespace: str = "fraud-eh-namespacer" #os.getenv('EVENTHUB_NAMESPACE')
    eventhub_name: str = "fraud-detect" #os.getenv('EVENTHUB_NAME')

settings = Settings()

