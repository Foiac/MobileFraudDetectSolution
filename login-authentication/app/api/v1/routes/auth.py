from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from app.services.auth_service import AuthService
from app.services.eventhub_service import EventHubService

router = APIRouter()
eventhub_service = EventHubService()
auth_service = AuthService(eventhub_service)

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login/")
async def login(request: LoginRequest):
    response = await auth_service.authenticate_user(request.username, request.password)
    if response['status'] == 'error':
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=response['message']
        )
    return response

@router.on_event("shutdown")
async def shutdown_eventhub():
    await eventhub_service.shutdown()
