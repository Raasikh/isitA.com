import sys
from pathlib import Path
from typing import Annotated, Literal

import sqlalchemy
from langchain_core.embeddings import DeterministicFakeEmbedding, Embeddings
from langchain_openai import OpenAIEmbeddings
from pydantic import (
    AnyUrl,
    BeforeValidator,
    EmailStr,
    ValidationError,
    computed_field,
)
from pydantic_settings import BaseSettings
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from app.core.constants import Defaults

env_path = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    # === Author====
    AUTHOR: str = Defaults.AUTHOR

    # === App Metadata ===
    APP_NAME: str = Defaults.APP_NAME
    APP_VERSION: str = Defaults.APP_VERSION
    APP_DESCRIPTION: str = Defaults.APP_DESCRIPTION
    APP_ENVIRONMENT: Literal["local", "staging", "production"] = Defaults.APP_ENVIRONMENT
    APP_FRONTEND_HOST: str = Defaults.APP_FRONTEND_HOST
    APP_HOST: str = Defaults.APP_HOST
    APP_PORT: int = Defaults.APP_PORT

    # === Auth ===
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Defaults.ACCESS_TOKEN_EXPIRE_MINUTES
    CLIENT_ID: str = Defaults.CLIENT_ID
    PROJECT_ID: str = Defaults.PROJECT_ID
    ISSUER_URL: AnyUrl = Defaults.ISSUER_URL

    # === Database ======
    DB_TYPE: Literal["postgresql"] = Defaults.DB_TYPE
    DB_HOST: str = Defaults.DB_HOST
    DB_PORT: int = Defaults.DB_PORT
    DB_NAME: str = Defaults.DB_NAME
    DB_USER: str = Defaults.DB_USER
    DB_PASSWORD: str = Defaults.DB_PASSWORD

    # === Vector Database =====
    DEFAULT_COLLECTION_NAME: str = Defaults.DEFAULT_COLLECTION_NAME

    # === API ===
    API_V1_STR: str = Defaults.API_V1_STR
    API_V2_STR: str = Defaults.API_V2_STR

    # === CORS ===
    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str,
        BeforeValidator(
            lambda v: (
                [i.strip() for i in v.split(",")]
                if isinstance(v, str) and not v.startswith("[")
                else v
            )
        ),
    ] = Defaults.BACKEND_CORS_ORIGINS
    BACKEND_CORS_ORIGINS_REGEX: str | None = None

    # === SMTP (optional) ===
    SMTP_TLS: bool = True
    SMTP_SSL: bool = False
    SMTP_PORT: int = 587
    SMTP_HOST: str | None = None
    SMTP_USER: str | None = None
    SMTP_PASSWORD: str | None = None
    EMAILS_FROM_EMAIL: EmailStr | None = None
    EMAILS_FROM_NAME: str | None = Defaults.APP_NAME

    # === Logging ===
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Defaults.LOG_LEVEL
    LOG_DIR: Path | None = Defaults.LOG_DIR
    LOG_JSON: bool = Defaults.LOG_JSON
    LOG_TO_FILE: bool = Defaults.LOG_TO_FILE

    # === Model ====
    LLM_API_KEY: str = "sk-...."  # pragma: allowlist secret
    LLM_MODEL_NAME: str = "gpt-4o"
    LLM_EMBEDDING_MODEL: str = "text-embedding-3-small"
    LLM_TEMPERATURE: float = 0.1

    # === Redis ===
    REDIS_URI: str = Defaults.REDIS_URI

    # =======Tracing & Evaluation========
    LANGSMITH_API_KEY: str = Defaults.LANGSMITH_API_KEY
    LANGSMITH_TRACING: bool = Defaults.LANGSMITH_TRACING
    LANGSMITH_PROJECT: str = Defaults.LANGSMITH_PROJECT

    LANGFUSE_SECRET_KEY: str = Defaults.LANGFUSE_SECRET_KEY
    LANGFUSE_PUBLIC_KEY: str = Defaults.LANGFUSE_PUBLIC_KEY
    LANGFUSE_HOST: str = Defaults.LANGFUSE_HOST

    @property
    def docs_enabled(self) -> bool:
        return self.APP_ENVIRONMENT in ["local", "staging"]

    @computed_field
    @property
    def DB_DRIVER(self) -> str:
        return {
            "mysql": "pymysql",
            "postgresql": "psycopg",
            "mssql": "pyodbc",
            "oracle": "cx_oracle",
            "cockroachdb": "psycopg",
            "mariadb": "pymysql",
            "firebird": "fdb",
            "sybase": "pyodbc",
            "sqlite": "",
        }[self.DB_TYPE]

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        if self.DB_TYPE == "sqlite":
            return f"sqlite:///{self.DB_NAME}"
        return (
            f"{self.DB_TYPE}+{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @computed_field
    @property
    def DEFAULT_EMBEDDINGS(self) -> Embeddings:
        if self.APP_ENVIRONMENT == "production":
            return OpenAIEmbeddings(api_key=self.LLM_API_KEY, model=self.LLM_EMBEDDING_MODEL)
        return DeterministicFakeEmbedding(size=1536)

    model_config = {"env_file": env_path, "case_sensitive": True}


class ConfigLoader:
    def __init__(self, settings_class: type[BaseSettings]):
        self.settings_class = settings_class
        self.console = Console(stderr=True)

    def _handle_validation_error(self, e: ValidationError):
        error_table = Table(box=None, show_header=False)
        error_table.add_column(style="bold magenta")
        error_table.add_column()

        for error in e.errors():
            field = ".".join(map(str, error["loc"]))
            error_table.add_row(f"'{field}'", error["msg"])

        error_panel = Panel(
            error_table,
            title="[bold red] ❌ Critical Configuration Error",
            subtitle="[dim]Check .env file or environment variables[/dim]",
            border_style="red",
            title_align="left",
            padding=(0, 1),
        )
        self.console.print(error_panel)
        sys.exit(1)

    def load(self) -> BaseSettings | None:
        try:
            return self.settings_class()

        except ValidationError as e:
            self._handle_validation_error(e)


class StartupChecker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.console = Console(stderr=True)

    def _check_env_file(self):
        """Checks for the .env file it uses the default configurations created in the constants file if we did not configure .env ="""
        if Path(".env").exists():
            message = "[bold green]✅ `.env` file found and loaded.[/bold green]"
            panel_color = "green"
        else:
            message = "[bold yellow]⚠️ No `.env` file found.[/bold yellow]\n[dim]Using default configuration variables ...[/dim]"
            panel_color = "yellow"
        self.console.print(Panel(message, expand=False, border_style=panel_color))

    def _check_db_connection(self):
        """This function should check database connection and show error if there is no connections otherwise ignore"""
        if self.settings.DB_TYPE == "sqlite":
            return
        try:
            engine = sqlalchemy.create_engine(self.settings.DATABASE_URL)
            with engine.connect():
                pass
        except Exception as e:
            error_message = (
                f"[bold red]🛑 Database Connection Failed[/bold red]\n\n[yellow]Error:[/yellow] {e}"
            )
            self.console.print(
                Panel(
                    error_message,
                    expand=False,
                    border_style="red",
                    title="[bold red]Connection Error[/bold red]",
                )
            )

    def run(self):
        self._check_env_file()
        self._check_db_connection()


settings = ConfigLoader(Settings).load()