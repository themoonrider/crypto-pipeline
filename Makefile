.PHONY: help build up down restart logs ps clean \
        trigger trigger-backfill dbt-run dbt-test dbt-docs \
        gcloud-login gcloud-set-project gcloud-adc gcs-create-bucket bq-setup

# ─── Default ──────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Market Data Pipeline (GCS + BigQuery) — Commands"
	@echo ""
	@echo "  GCP Setup (run once)"
	@echo "    make gcloud-login   Authenticate with Google Cloud"
	@echo "    make gcloud-set-project PROJECT=<id>  Set default GCP project"
	@echo "    make gcloud-adc     Set up Application Default Credentials"
	@echo "    make gcs-create-bucket BUCKET=<name>  Create GCS bucket"
	@echo "    make bq-setup       Create BigQuery raw_market_data dataset"
	@echo ""
	@echo "  Docker"
	@echo "    make build          Build all Docker images"
	@echo "    make up             Start all services"
	@echo "    make down           Stop all services"
	@echo "    make restart        Restart all services"
	@echo "    make logs           Tail all logs"
	@echo "    make ps             Show service status"
	@echo "    make clean          Remove volumes and images"
	@echo ""
	@echo "  Pipeline"
	@echo "    make trigger        Trigger the daily pipeline (rolling 365d)"
	@echo "    make trigger-backfill START=2024-01-01 END=2024-03-31"
	@echo "    make dbt-run        Run dbt models manually"
	@echo "    make dbt-test       Run dbt tests manually"
	@echo "    make dbt-docs       Generate and serve dbt documentation"
	@echo ""

# ─── GCP Setup ───────────────────────────────────────────────────────────────
gcloud-login:
	gcloud auth login
	@echo ""
	@echo "  Next: make gcloud-set-project PROJECT=<YOUR_PROJECT_ID>"
	@echo ""

gcloud-set-project:
	@if [ -z "$(PROJECT)" ]; then \
		echo "Usage: make gcloud-set-project PROJECT=your-project-id"; \
		exit 1; \
	fi
	gcloud config set project $(PROJECT)
	@echo ""
	@echo "  Project set to $(PROJECT)"
	@echo "  Next: make gcloud-adc"
	@echo ""

gcloud-adc:
	gcloud auth application-default login
	@echo ""
	@echo "  ADC credentials saved. Airflow and dbt will use these automatically."
	@echo ""

gcs-create-bucket:
	@if [ -z "$(BUCKET)" ]; then \
		echo "Usage: make gcs-create-bucket BUCKET=your-bucket-name"; \
		exit 1; \
	fi
	gcloud storage buckets create gs://$(BUCKET) --location=US
	@echo ""
	@echo "  Bucket gs://$(BUCKET) created"
	@echo ""

bq-setup:
	@echo "Creating BigQuery datasets..."
	bq query --use_legacy_sql=false < bigquery/setup.sql
	@echo "BigQuery setup complete (staging, marts, test_results datasets created by dbt on first run)"

# ─── Docker ──────────────────────────────────────────────────────────────────
build:
	@cp -n .env.example .env 2>/dev/null || true
	docker compose build

up:
	@cp -n .env.example .env 2>/dev/null || true
	docker compose up -d
	@echo ""
	@echo "  Services started"
	@echo "  Airflow:  http://localhost:8080  (admin/admin)"
	@echo ""

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

clean:
	docker compose down -v --rmi local
	@echo "Volumes and images removed"

# ─── Pipeline ─────────────────────────────────────────────────────────────────
trigger:
	docker compose exec airflow-scheduler \
		airflow dags trigger market_data_pipeline

trigger-backfill:
	@if [ -z "$(START)" ] || [ -z "$(END)" ]; then \
		echo "Usage: make trigger-backfill START=2024-01-01 END=2024-03-31"; \
		exit 1; \
	fi
	docker compose exec airflow-scheduler \
		airflow dags trigger market_data_pipeline \
		--conf '{"fetch_start": "$(START)", "fetch_end": "$(END)"}'

dbt-run:
	docker compose exec airflow-scheduler \
		dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --full-refresh

dbt-test:
	docker compose exec airflow-scheduler \
		dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

dbt-docs:
	docker compose exec airflow-scheduler \
		dbt docs generate --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt
	docker compose exec airflow-scheduler \
		dbt docs serve --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --host 0.0.0.0 --port 8081
