from datetime import datetime

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}