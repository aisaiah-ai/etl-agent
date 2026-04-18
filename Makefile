.PHONY: setup up down test test-local run discover clean lint

# ── Setup ────────────────────────────────────────────────────────────────
setup:
	python -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	@echo "Run: source .venv/bin/activate"

# ── Docker Compose ───────────────────────────────────────────────────────
up:
	docker compose up -d --wait
	@echo "Services running:"
	@echo "  PostgreSQL (Redshift):  localhost:5439"
	@echo "  LocalStack (Glue/S3):  localhost:4566"

down:
	docker compose down

down-clean:
	docker compose down -v

# ── Testing ──────────────────────────────────────────────────────────────
test:
	pytest tests/unit/ -v

test-local: up
	ETL_LOCAL=1 pytest tests/integration/ -v

# ── Local Pipeline ───────────────────────────────────────────────────────
run: up
	./scripts/run_local.sh --skip-docker

discover: up
	./scripts/run_local.sh --skip-docker --discover-only

# ── Code Quality ─────────────────────────────────────────────────────────
lint:
	ruff check pipelines/ agents/ tests/
	black --check pipelines/ agents/ tests/

format:
	ruff check --fix pipelines/ agents/ tests/
	black pipelines/ agents/ tests/

# ── Cleanup ──────────────────────────────────────────────────────────────
clean:
	./scripts/teardown.sh

# ── Query local Redshift ─────────────────────────────────────────────────
psql:
	PGPASSWORD=local_dev_password psql -h localhost -p 5439 -U admin -d etl_agent_db

# ── Query local Glue catalog ─────────────────────────────────────────────
glue-tables:
	aws --endpoint-url=http://localhost:4566 glue get-tables --database-name etl_agent_local --query 'TableList[].Name' --output table
