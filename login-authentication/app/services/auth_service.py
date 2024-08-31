class AuthService:
    def __init__(self, eventhub_service):
        self.eventhub_service = eventhub_service

    async def authenticate_user(self, username: str, password: str):
        # Aqui você pode adicionar a lógica real de autenticação.
        # Por exemplo, consultar um banco de dados ou um serviço de autenticação.
        if username == "user" and password == "password":
            # Publicar um evento no Event Hub de forma assíncrona.
            await self.eventhub_service.publish_event(f"User {username} logged in")
            return {"status": "success", "message": "Login successful"}
        return {"status": "error", "message": "Invalid credentials"}
