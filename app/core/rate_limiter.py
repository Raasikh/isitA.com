from fastapi import Request
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.responses import JSONResponse

from app.core.logging import logger

limiter = Limiter(key_func=get_remote_address, default_limits=["20/minute"])


async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """
    Logs the rate limit event and returns a custom 429 JSON response.
    """
    logger.warning(
        f"Rate limit exceeded: "
        f"Client IP='{request.client.host}' "
        f"Path='{request.url.path}' "
        f"Limit='{exc.limit.limit}'"
    )
    return JSONResponse(
        status_code=429, content={"detail": "Rate limit exceeded. Try again later."}
    )