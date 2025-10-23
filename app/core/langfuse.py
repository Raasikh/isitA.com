import os

from langfuse import get_client
from langfuse.langchain import CallbackHandler

from app.core.config import settings
from app.core.logging import logger


def init_langfuse():
    os.environ["LANGFUSE_PUBLIC_KEY"] = settings.LANGFUSE_PUBLIC_KEY
    os.environ["LANGFUSE_SECRET_KEY"] = settings.LANGFUSE_SECRET_KEY
    os.environ["LANGFUSE_HOST"] = settings.LANGFUSE_HOST

    client = get_client()
    handler = CallbackHandler()

    if not client.auth_check():
        logger.critical("Langfuse authentication failed! Check your credentials and host.")

    logger.info("Langfuse client is authenticated and ready!")

    return client, handler