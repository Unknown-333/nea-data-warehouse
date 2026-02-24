# =============================================================
# NEA Data Warehouse — Makefile
# One-command operations for the entire pipeline
# =============================================================

# Load .env so dbt can read POSTGRES_PASSWORD etc.
-include .env
export

.PHONY: help up down extract load dbt-run dbt-test pipeline clean status

# Default target
help:  ## Show this help
	@echo.
	@echo   NEA Data Warehouse - Available Commands
	@echo   ========================================
	@echo   make up          Start Docker services (PostgreSQL + Metabase)
	@echo   make down        Stop Docker services
	@echo   make extract     Download PDFs and extract to CSV
	@echo   make load        Load CSVs into PostgreSQL
	@echo   make dbt-run     Run dbt transformations
	@echo   make dbt-test    Run dbt tests
	@echo   make pipeline    Run full pipeline (extract + load + dbt)
	@echo   make status      Check Docker service health
	@echo   make clean       Reset everything (WARNING: deletes data)
	@echo.

# ─── Docker ───────────────────────────────────
up:  ## Start PostgreSQL + Metabase
	docker-compose up -d
	@echo [OK] Services starting... check with 'make status'

down:  ## Stop all services
	docker-compose down
	@echo [OK] Services stopped.

status:  ## Check service health
	docker-compose ps

# ─── ETL Pipeline ─────────────────────────────
extract:  ## Download PDFs and extract to CSVs
	python -m extractor.extract
	@echo [OK] Extraction complete. CSVs in data/bronze/

load:  ## Load CSVs into PostgreSQL
	python db/load.py
	@echo [OK] Data loaded into PostgreSQL.

# ─── dbt ──────────────────────────────────────
dbt-run:  ## Run dbt models
	cd dbt_nea && dbt run --profiles-dir .
	@echo [OK] dbt models built.

dbt-test:  ## Run dbt tests
	cd dbt_nea && dbt test --profiles-dir .
	@echo [OK] All dbt tests passed.

# ─── Combined ─────────────────────────────────
pipeline: extract load dbt-run dbt-test  ## Run full pipeline
	@echo.
	@echo [DONE] Full pipeline completed successfully!
	@echo   - CSVs extracted to data/bronze/
	@echo   - Data loaded into PostgreSQL
	@echo   - dbt models built and tested
	@echo   - Dashboard at http://localhost:3000
	@echo.

# ─── Maintenance ──────────────────────────────
clean:  ## Reset everything (WARNING: deletes all data)
	docker-compose down -v
	if exist data\raw_pdfs rd /s /q data\raw_pdfs
	if exist data\bronze rd /s /q data\bronze
	@echo [OK] All data cleaned.

install:  ## Install Python dependencies
	pip install -r requirements.txt
	@echo [OK] Dependencies installed.
