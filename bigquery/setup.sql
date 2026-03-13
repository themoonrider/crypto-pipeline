-- ============================================================
-- BigQuery Setup — Create raw_market_data dataset used in loading step of the pipeline. Run this once before starting the pipeline.
-- ============================================================
-- Run with: bq query --use_legacy_sql=false < bigquery/setup.sql

CREATE SCHEMA IF NOT EXISTS raw_market_data
    OPTIONS (location = 'US');
