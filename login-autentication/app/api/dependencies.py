from app.infrastructure.repositories import UserRepository
from app.infrastructure.eventhub import EventHub
from app.services.auth_service import AuthService

def get_auth_service():
    user_repository = UserRepository()
    event_hub = EventHub()
    return AuthService(user_repository, event_hub)