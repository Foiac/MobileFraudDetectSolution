from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    azure_tenant_id: str = "333cdf23-94bc-450f-a816-9989c891494b" #os.getenv('AZURE_TENANT_ID')
    azure_client_id: str = "0bc1b99e-9403-4708-8019-df5ab484380b" #os.getenv('AZURE_CLIENT_ID')
    azure_client_secret: str = "bss8Q~534wTOlXqyh.eE2Ya6D3I9S4meioBY3dcR" #os.getenv('AZURE_CLIENT_SECRET')
    eventhub_namespace: str = "eventhub-dev-stbr" #os.getenv('EVENTHUB_NAMESPACE')
    eventhub_name: str = "frauddetect" #os.getenv('EVENTHUB_NAME')

settings = Settings()

