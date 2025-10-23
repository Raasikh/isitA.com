import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.postgres.aio import (
    AsyncPostgresSaver,  # short-term memory : checkpoints -> resume conversations
)
from langgraph.store.memory import InMemoryStore
from langgraph.store.postgres.aio import (
    AsyncPostgresStore,  # long-term memory : semantic, procedural and episodic
)
from langmem import ReflectionExecutor, create_memory_store_manager
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.core.config import settings
from app.core.logging import logger
from app.schemas.memory import QueryEpisode, SQLPattern, UserProfile
from app.services.memory import MemoryTools

_pool: asyncpg.Pool | None = None


def create_profile_manager(store):
    return create_memory_store_manager(
        settings.LLM_MODEL_NAME,
        namespace=("users", "{user_id}", "profile"),
        store=store,
        schemas=[UserProfile],
        instructions="""
            Extract and update user profile information including:
            - Personal details (name, role, department, company)
            - Technical skills and expertise levels
            - Communication preferences
            - Commonly used tools/tables
            Update existing profile with new information, don't create duplicates.
        """,
        enable_inserts=False,
        enable_deletes=False,
    )


def create_main_memory_manager(store):
    return create_memory_store_manager(
        settings.LLM_MODEL_NAME,
        namespace=("memories", "{user_id}", "{memory_type}"),
        store=store,
        schemas=[QueryEpisode, SQLPattern],
        instructions="""
            Extract memories based on schema:
            - QueryEpisode: store each user query, generated SQL, execution success, tables, type, and feedback.
            - SQLPattern: store reusable SQL templates, use cases, and success rates.
        """,
        enable_inserts=True,
        enable_deletes=False,
    )


def init_in_memory_tools():
    checkpointer = MemorySaver()
    store = InMemoryStore()
    profile_manager = create_profile_manager(store)
    memory_manager = create_main_memory_manager(store)
    main_reflection_executor = ReflectionExecutor(memory_manager, store=store)
    profile_reflection_executor = ReflectionExecutor(profile_manager, store=store)
    memory_tools = MemoryTools(store, profile_reflection_executor, main_reflection_executor)
    return store, checkpointer, memory_tools


@asynccontextmanager
async def init_memory() -> AsyncGenerator[dict[str, AsyncPostgresSaver], None]:
    os.environ["OPENAI_API_KEY"] = settings.LLM_API_KEY
    uri = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    pool = AsyncConnectionPool(conninfo=uri, kwargs={"autocommit": True, "row_factory": dict_row})
    async with pool:
        checkpointer = AsyncPostgresSaver(pool)
        store = AsyncPostgresStore(pool, index={"embed": settings.DEFAULT_EMBEDDINGS, "dims": 1536})
        await checkpointer.setup()
        await store.setup()

        profile_manager = create_profile_manager(store)
        memory_manager = create_main_memory_manager(store)

        main_reflection_executor = ReflectionExecutor(memory_manager, store=store)
        profile_reflection_executor = ReflectionExecutor(profile_manager, store=store)
        logger.info("Agent memory initialized with separate profile and general memory managers")

        yield {
            "checkpointer": checkpointer,
            "store": store,
            "main_reflection_executor": main_reflection_executor,
            "profile_reflection_executor": profile_reflection_executor,
        }