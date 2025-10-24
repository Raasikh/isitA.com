from fastapi import APIRouter

from app.api.routes import chat, health, collections, documents

router = APIRouter()


router.include_router(chat.router, prefix="/chat", tags=["chat"])
router.include_router(health.router, prefix="/health", tags=["health"])
router.include_router(collections.router, prefix="/collections", tags=["collections"])
router.include_router(documents.router, prefix="/documents", tags=["documents"])
__all__ = ["router"]