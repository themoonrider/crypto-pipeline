"""
market_data_pipeline DAG
────────────────────────
Orchestrates:
  1. Parallel ingestion: Massive API (stocks/forex/index) + CoinGecko (BTC)
  2. Load raw JSON from GCS → BigQuery raw tables
  3. dbt run (BashOperator): staging → marts (star schema, metrics)
  4. dbt test (BashOperator): data quality checks
  5. Export summary reports from BigQuery to GCS as CSV

Schedule: Daily at 6 AM UTC (after Asian market close, before EU open)

Configurable date ranges via DAG Params (shown in Trigger UI):
  - fetch_start  (YYYY-MM-DD, optional — defaults to data_interval_start)
  - fetch_end    (YYYY-MM-DD, optional — defaults to data_interval_end)

Connections required:
  - google_cloud_default: GCP connection (uses ADC)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.massive_operator import MassiveAPIOperator
from operators.coingecko_operator import CoinGeckoOperator


GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
DBT_DIR = "/opt/airflow/dbt"


# ─── Default Args ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30)
}


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="market_data_pipeline",
    description="Ingest traditional assets + Bitcoin, transform with dbt, export reports",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["market-data", "bitcoin", "dbt", "bigquery"],
    doc_md=__doc__,
    params={
        "fetch_start": Param(
            default="",
            type="string",
            description="Fetch start date (YYYY-MM-DD). Leave empty for data_interval_start.",
        ),
        "fetch_end": Param(
            default="",
            type="string",
            description="Fetch end date (YYYY-MM-DD). Leave empty for data_interval_end.",
        ),
    },
) as dag:

    # ── Start ─────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Date templates ────────────────────────────────────────────────────────
    _mv_start = "{{ params.fetch_start if params.fetch_start else data_interval_start.strftime('%Y-%m-%d') }}"
    _mv_end = "{{ params.fetch_end if params.fetch_end else data_interval_end.strftime('%Y-%m-%d') }}"

    # ── Ingestion (parallel) — fetch from APIs, save to GCS ──────────────────
    ingest_massive = MassiveAPIOperator(
        task_id="ingest_massive_api",
        symbols=["AAPL", "GOOGL", "MSFT", "SPY", "C:EURUSD", "C:GBPUSD"],
        fetch_start=_mv_start,
        fetch_end=_mv_end,
        gcs_bucket=GCS_BUCKET,
        gcp_conn_id="google_cloud_default",
        massive_api_key=os.getenv("MASSIVE_API_KEY", ""),
        pool="default_pool",
        sla=timedelta(minutes=30),
    )

    _cg_end = "{{ params.fetch_end if params.fetch_end else data_interval_end.strftime('%Y-%m-%d') }}"
    _cg_start = "{{ macros.ds_add(params.fetch_end if params.fetch_end else data_interval_end.strftime('%Y-%m-%d'), -364) }}"

    ingest_coingecko = CoinGeckoOperator(
        task_id="ingest_coingecko_api",
        coin_ids=["bitcoin"],
        fetch_start=_cg_start,
        fetch_end=_cg_end,
        gcs_bucket=GCS_BUCKET,
        gcp_conn_id="google_cloud_default",
        api_key=os.getenv("COINGECKO_API_KEY", ""),
        pool="default_pool",
        sla=timedelta(minutes=30),
)
    # pass XComs (GCS keys) to downstream tasks
    @task 
    def extract_gcs_keys(gcs_map: dict) -> list[str]:
        return list(gcs_map.values())

    massive_gcs_keys = extract_gcs_keys(ingest_massive.output)
    coingecko_gcs_keys = extract_gcs_keys(ingest_coingecko.output)

    _schema_dir = Path(__file__).parent / "schema"
    _massive_schema = json.loads((_schema_dir / "massive_prices.json").read_text())
    _coingecko_schema = json.loads((_schema_dir / "coingecko_market_chart.json").read_text())

    load_massive_to_bq = GCSToBigQueryOperator(
        task_id="load_massive_to_bq",
        bucket=GCS_BUCKET,
        source_objects=massive_gcs_keys,
        destination_project_dataset_table=f"{GCP_PROJECT}.raw_market_data.massive_prices",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema_fields=_massive_schema,
        time_partitioning={"type": "DAY", "field": "__de_processed_at"},
        cluster_fields=["symbol"],
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )

    load_coingecko_to_bq = GCSToBigQueryOperator(
        task_id="load_coingecko_to_bq",
        bucket=GCS_BUCKET,
        source_objects=coingecko_gcs_keys,
        destination_project_dataset_table=f"{GCP_PROJECT}.raw_market_data.coingecko_market_chart",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema_fields=_coingecko_schema,
        time_partitioning={"type": "DAY", "field": "__de_processed_at"},
        cluster_fields=["coin_id"],
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )


    

    
    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> [ingest_massive, ingest_coingecko]
    massive_gcs_keys >> load_massive_to_bq
    coingecko_gcs_keys >> load_coingecko_to_bq
