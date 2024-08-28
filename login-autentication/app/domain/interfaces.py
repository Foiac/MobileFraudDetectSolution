from abc import ABC, abstractmethod

class UserRepositoryInterface(ABC):
    @abstractmethod
    async def find_user_by_username(self, username: str):
        pass

class EventHubInterface(ABC):
    @abstractmethod
    async def publish_event(self, event: dict):
        pass