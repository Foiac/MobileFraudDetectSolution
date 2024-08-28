from app.domain.interfaces import UserRepositoryInterface, EventHubInterface

class AuthService:
    def __init__(self, user_repository: UserRepositoryInterface, event_hub: EventHubInterface):
        self.user_repository = user_repository
        self.event_hub = event_hub

    async def authenticate_user(self, username: str, password: str):
        user = await self.user_repository.find_user_by_username(username)
        if not user or user.password != password:
            return None
        # Envia dados de autenticação para o Event Hub
        await self.event_hub.publish_event({"user": username, "status": "authenticated"})
        return user