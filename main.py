from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.api.routes import router
from app.core.app_state import app_state
from app.core.config import settings, StartupChecker
from app.core.database import create_system_collections
from app.core.langfuse import init_langfuse
from app.core.rate_limiter import limiter, rate_limit_exceeded_handler


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator:
    StartupChecker(settings).run()
    await create_system_collections()
    # client, app_state.langfuse_handler = init_langfuse()
    yield


app = FastAPI(
    title=settings.APP_NAME,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url="/redoc" if settings.docs_enabled else None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
    lifespan=lifespan

)

app.state.limiter = limiter  # type: ignore[attr-defined]
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)  # type: ignore[attr-defined]
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router, prefix=settings.API_V1_STR)



if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=False,
    )