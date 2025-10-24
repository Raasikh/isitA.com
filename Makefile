.PHONY: help start stop restart rebuild status logs health setup migrate format lint clean studio inspector kill-mcp-port

# ==============================================================================
# Help
# ==============================================================================

help: ## Shows this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-20s %s\n", $$1, $$2}'

# ==============================================================================
# Service Management (Docker)
# ==============================================================================

start: ## Start all services in detached mode
	docker compose up -d

rebuild: ## Rebuild services without cache and start
	docker compose build --no-cache
	docker compose up -d

stop: ## Stop and remove all services
	docker compose down

restart: ## Restart all running services
	docker compose restart

status: ## Show the status of all services
	docker compose ps

logs: ## Follow the logs of all services
	docker compose logs -f

clean: ## Stop, remove all containers, and prune docker system
	docker compose down -v
	docker system prune -f

# ==============================================================================
# Health & Utilities
# ==============================================================================

health: ## Check the health of all running services
	@echo "--- FastAPI Server (API) ---"
	@curl -s http://localhost:8005/api/v1/health | jq .

# Generic utility to kill a process by port
PORT ?= 8000
kill-port: ## Find and kill a process by port (default: $(PORT))
	@pid=$$(lsof -ti:$(PORT)); \
	if [ -n "$$pid" ]; then \
	   echo "Port $(PORT) is used by PID $$pid. Terminating..."; \
	   kill -9 $$pid; \
	   echo "Process $$pid terminated."; \
	else \
	   echo "No process found on port $(PORT)."; \
	fi
# ==============================================================================
# Development Workflow
# ==============================================================================

setup: ## Create venv and install dependencies using uv
	@echo "Setting up virtual environment and installing dependencies..."
	@uv sync
	@echo "\n Setup complete. Activate the environment with:"
	@echo "   source .venv/bin/activate"

migrate: ## Run database migrations using alembic
	uv run alembic upgrade head

format: ## Format code using ruff
	uv run ruff format app/

lint: ## Lint and type-check code with ruff
	uv run ruff check . --fix app/


# ==============================================================================
# LangGraph & MCP Server
# ==============================================================================

studio: ## Run the LangGraph studio UI
	uv run langgraph dev

MCP_SERVER_SCRIPT ?= ./mcp/server.py
inspector: ## Run the MCP development server
	uv run --env-file .env -- env PYTHONPATH=. fastmcp dev $(MCP_SERVER_SCRIPT)
