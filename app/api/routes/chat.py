from datetime import datetime

from fastapi import APIRouter
from starlette.requests import Request
from app.core.logging import logger


router = APIRouter()


@router.post("")
async def chat(
    request: Request

):
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}