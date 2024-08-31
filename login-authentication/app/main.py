from fastapi import FastAPI
from app.api.v1.routes import auth

app = FastAPI()

app.include_router(auth.router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
