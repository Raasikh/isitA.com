from fastapi import APIRouter, Depends
from langchain_core.runnables import RunnableConfig
from starlette.requests import Request
from starlette.responses import StreamingResponse

from app.core.auth import get_enhanced_user
from app.core.logging import logger
from app.schemas.chat import ChatRequest
from app.utils.util import Util

router = APIRouter()


@router.post("")
async def chat(
    request: Request,
    input: ChatRequest,
    user: dict = Depends(get_enhanced_user),
):
    logger.info(f"Received request: {input}")
    logger.info(f"User info: {user}")

    config = RunnableConfig(configurable={"thread_id": input.thread_id, "user_id": user["sub"]})
    return StreamingResponse(
        Util.stream_generator(input.messages, config),
        media_type="text/event-stream",
    )
