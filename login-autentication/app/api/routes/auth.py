from fastapi import APIRouter, Depends
from app.services.auth_service import AuthService
from app.api.dependencies import get_auth_service

router = APIRouter()

@router.get("/teste")
async def teste():
    return {"message": "Hello World"}

@router.post("/login")
async def login(username: str, password: str, auth_service: AuthService = Depends(get_auth_service)):
    user = await auth_service.authenticate_user(username, password)
    if not user:
        return {"error": "Invalid credentials"}
    return {"message": "Login successful", "user": user.username}