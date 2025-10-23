from fastapi import APIRouter

from app.api.routes import chat, health

router = APIRouter()


router.include_router(chat.router, prefix="/chat", tags=["chat"])
router.include_router(health.router, prefix="/health", tags=["health"])

__all__ = ["router"]