# Market Data Pipeline — Traditional Assets vs Bitcoin

A data pipeline comparing traditional financial assets against Bitcoin over the last 365 days. Built with Airflow, Google Cloud Storage, BigQuery, and dbt.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow (Orchestrator)                   │
│  ┌─────────────────┐         ┌──────────────────────────────┐   │
│  │ MassiveAPI       │         │ CoinGeckoOperator           │   │
│  │ Operator         │         │ (Bitcoin OHLC)              |   │
│  │ (Stocks/FX/SPY) │         └──────────────┬───────────────┘   │
│  └────────┬────────┘                        │                   │
│           │      Raw JSON                   │ Raw JSON          │
│           ▼                                 ▼                   │
│  ┌────────────────────────────────────────────────────┐         │
│  │           Google Cloud Storage (GCS)                │        │
│  │  raw/massive/{symbol}/...json                       │        │
│  │  raw/coingecko/bitcoin/...json                      │        │
│  └────────────────────────────────────────────────────┘         │
│           │                                 │                   │
│           │    GCSToBigQuery                │ GCSToBigQuery     │
│           ▼                                 ▼                   │
│  ┌────────────────────────────────────────────────────┐         │
│  │              BigQuery Data Warehouse               │         │
│  │  raw_market_data.massive_prices                    │         │
│  │  raw_market_data.coingecko_market_chart            │         │
│  └─────────────────────┬──────────────────────────────┘         │
│                         │                                       │
│           ┌─────────────▼─────────────┐                         │
│           │   dbt (BashOperator)      │                         │
│           │   staging → marts         │                         │
│           └─────────────┬─────────────┘                         │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────┐        │
│  │  Star Schema (marts dataset)                         │        │
│  │  dim_assets  dim_dates  fact_daily_prices             │        │
│  │  rolling_performance  btc_relative_performance        │        │
│  │  asset_performance_summary  volatility_correlation    │        │
│  └─────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Assets | Client Method |
|--------|--------|---------------|
| Massive API | AAPL, GOOGL, MSFT, SPY, EUR/USD, GBP/USD | [`RESTClient.list_aggs()`](https://deepwiki.com/massive-com/client-python/3.2.1-aggregates-(bars)#list_aggs---paginated-time-series-aggregates) |
| CoinGecko API | Bitcoin | [`CoinGeckoAPI.coins.market_chart.get_range()`](https://github.com/coingecko/coingecko-python/blob/main/api.md) |

## Prerequisites

- Docker + Docker Compose v2
- Google Cloud SDK (`gcloud`) installed
- A GCP project with billing enabled
- API keys for Massive and CoinGecko

## Setup

### 1. Authenticate with Google Cloud

```bash
# Login to your Google account
make gcloud-login

# Set your default project
make gcloud-set-project PROJECT=<YOUR_GCP_PROJECT_ID>

# Set up Application Default Credentials (used by Airflow and dbt)
make gcloud-adc
```

### 2. Create GCS Bucket

```bash
make gcs-create-bucket BUCKET=<YOUR_BUCKET_NAME>
```

### 3. Set Up BigQuery

```bash
# Create the raw_market_data dataset (used by the loading step)
make bq-setup
```

This creates the `raw_market_data` dataset. The `staging`, `marts`, and `test_results` datasets are created automatically by dbt on first run.

### 4. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your values:
```
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-bucket-name
BQ_LOCATION=US
MASSIVE_API_KEY=your_key
COINGECKO_API_KEY=your_key
```

### 5. Build and Start

```bash
make build
make up
```

### 6. Access Airflow

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |

## Running the Pipeline

**Option A: Trigger via Airflow UI**
1. Open http://localhost:8080
2. Enable the `market_data_pipeline` DAG
3. Click the play button to trigger a manual run

**Option B: CLI trigger**
```bash
make trigger
```

**Option C: With custom date range**
```bash
make trigger-backfill START=2024-01-01 END=2024-03-31
```

## DAG Flow

```
start
  ├── ingest_massive_api ──► extract_gcs_keys ──► load_massive_to_bq ──┐
  └── ingest_coingecko_api ► extract_gcs_keys ──► load_coingecko_to_bq ┘
                                                                        │
                                                                   dbt_run
                                                                        │
                                                                   dbt_test
                                                                        │
                                                              alert (on failure)
                                                                        │
                                                                       end
```

## Data Model

### Star Schema

```
              dim_dates ──────────────────┐
                                          │
dim_assets ──── fact_daily_prices ────────┤
                    │                     │
                    ├── rolling_performance
                    ├── btc_relative_performance
                    ├── asset_performance_summary
                    └── volatility_correlation_summary
```

### Tables

| Table | Dataset | Description |
|-------|---------|-------------|
| `massive_prices` | raw_market_data | Raw OHLCV from Massive API |
| `coingecko_market_chart` | raw_market_data | Raw BTC data from CoinGecko |
| `stg_massive_prices` | staging | Cleaned Massive data |
| `stg_coingecko_prices` | staging | Cleaned CoinGecko data |
| `dim_assets` | marts | Asset dimension (7 assets) |
| `dim_dates` | marts | Date dimension spine |
| `fact_daily_prices` | marts | Core daily price fact with returns |
| `rolling_performance` | marts | 7/30/90/180/365d rolling metrics |
| `btc_relative_performance` | marts | Traditional assets vs BTC comparison |
| `asset_performance_summary` | marts | One-row-per-asset summary |
| `volatility_correlation_summary` | marts | Pairwise correlations |

## Data Quality

Tests are defined in `dbt/models/schema.yml` using `dbt_expectations` and run via `dbt test`:

- **Completeness**: Expected row counts, distinct asset counts per model
- **Accuracy**: Price range checks, return bounds, volatility non-negative, correlation in [-1, 1]
- **Consistency**: Unique key constraints, no Bitcoin in relative performance model, data gap monitoring

Failing test results are stored in the `test_results` dataset in BigQuery (`+store_failures: true`).

## Sample Data

The `sample_data/` directory contains example CSV outputs from dbt models for reference.

## Sample Queries

### Best performers vs Bitcoin (365d)

```sql
SELECT
    symbol,
    return_365d,
    btc_return_365d,
    outperformance_vs_btc_365d,
    volatility_365d,
    rank_return_365d
FROM marts.asset_performance_summary
ORDER BY outperformance_vs_btc_365d DESC;
```

### Correlation matrix

```sql
SELECT symbol_a, symbol_b, correlation
FROM marts.volatility_correlation_summary
ORDER BY ABS(correlation) DESC;
```

## Development

### Run dbt manually

```bash
make dbt-run
make dbt-test
make dbt-docs
```