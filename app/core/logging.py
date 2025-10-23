import sys
from pathlib import Path

from loguru import logger

from app.core.config import settings

LOG_LEVEL = settings.LOG_LEVEL
SERIALIZE = settings.LOG_JSON
LOG_TO_FILE = settings.LOG_TO_FILE
LOG_DIR = Path(settings.LOG_DIR)


class LoggerConfig:
    def __init__(self) -> None:
        self.log_level = LOG_LEVEL
        self.serialize = SERIALIZE

    def configure(self) -> None:
        logger.remove()
        logger.add(
            sys.stdout,
            level=self.log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{file}:{function}:{line}</cyan> - <level>{message}</level>",
            colorize=True,
            backtrace=True,
            diagnose=settings.APP_ENVIRONMENT,
            enqueue=True,
            serialize=self.serialize,
        )

        if LOG_TO_FILE:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            logger.add(
                LOG_DIR / "app.log",
                level="INFO",
                rotation="10 MB",
                retention="10 days",
                compression="zip",
                backtrace=True,
                diagnose=settings.APP_ENVIRONMENT,
                enqueue=True,
                serialize=self.serialize,
            )

    @staticmethod
    def setup_exception_hook() -> None:
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            logger.opt(exception=(exc_type, exc_value, exc_traceback)).critical(
                "Uncaught exception"
            )

        sys.excepthook = handle_exception


logger_config = LoggerConfig()
logger_config.configure()
logger_config.setup_exception_hook()