from pathlib import Path


class Defaults:
    AUTHOR = "Ronald N. Kanyepi & Raasikh Ahmed"
    VERSION = "0.0.1"

    # === App Metadata ===
    APP_NAME = "Default app name"
    APP_VERSION = "0.1.0"
    APP_DESCRIPTION = "This is the description of my application"
    APP_ENVIRONMENT = "local"
    APP_HOST = "127.0.0.1"
    APP_PORT = 8005

    # === Frontend Host ===
    APP_FRONTEND_HOST = "http://localhost:3000"

    # === Database ======
    DB_TYPE = "postgresql"
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = "postgres"
    DB_USER = "postgres"
    DB_PASSWORD = "postgres" #pragma: allowlist secret

    # =======Model======
    LLM_API_KEY = "sk-...."  # pragma: allowlist secret
    LLM_MODEL_NAME = "gpt-4o"
    LLM_TEMPERATURE = 0.1

    # == Vector Database ====
    DEFAULT_COLLECTION_NAME = "default_collection"

    # === Auth ===
    ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 8  # 8 days
    CLIENT_ID = ""
    PROJECT_ID = ""
    ISSUER_URL = "http://localhost:8000"

    # === API ===
    API_V1_STR = "/api/v1"
    API_V2_STR = "/api/v2"

    # === CORS ===
    BACKEND_CORS_ORIGINS = ["http://localhost:3000"]

    # === Logging ===
    LOG_LEVEL = "INFO"
    LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
    LOG_JSON = False
    LOG_TO_FILE = True

    # === Storage / Cache ===
    REDIS_URI = "redis://localhost:6379"

    # =======Tracing & Evaluation========
    LANGSMITH_API_KEY = "lsv2_......"  # pragma: allowlist secret
    LANGSMITH_TRACING = "true"
    LANGSMITH_PROJECT = "text-to-sql-agent"

    LANGFUSE_SECRET_KEY = "sk-lf-..."  # pragma: allowlist secret
    LANGFUSE_PUBLIC_KEY = "pk-lf-..."  # pragma: allowlist secret
    LANGFUSE_HOST = "https://cloud.langfuse.com"