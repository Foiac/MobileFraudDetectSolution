from app.domain.interfaces import UserRepositoryInterface

class UserRepository(UserRepositoryInterface):
    async def find_user_by_username(self, username: str):
        # Implementação para buscar o usuário no banco de dados
        pass