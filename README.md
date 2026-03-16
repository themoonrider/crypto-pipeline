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

More on table description and schema is found in `dbt/models/schema.yml`

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

## Written analysis 

### Best performers vs Bitcoin (1Y, YTD, 6M, 3M, 1M, 7D) - (Data as of 2026-03-13)

Sample query:
```sql
-- Best performers vs Bitcoin 1Y - GOOGL
SELECT
    symbol,
    return_365d,
    btc_return_365d,
    outperformance_vs_btc_365d
FROM dbt_marts.asset_performance_summary_latest
ORDER BY outperformance_vs_btc_365d DESC;
```
Results: Best performers vs Bitcoin over period of:
1Y - GOOGL
YTD - GOOGL
6M - GOOGL
3M - GOOGL
1M - C:GBPUSD
7D - GOOGL


### Current worth of a $1K USD vs Bitcoin investment made one year ago - (Data as of 2026-03-13)
date
2026-03-13 - 70544.43 USD/bitcoin
2025-03-13 - 83884.24 USD/bitcoin
A $1K USD investment in bitcoin on 2025-03-13 would have made a **loss** of 
(1000/83884.24)*70544.43 - 1000 =  -159 USD or -15.9% return


### Return from $100/month dollar-cost over 12 months vs initial lump sum to bitcoin
As of 2026-03-13. 
Case 1: Invested $100*12 = $1200 USD on 2025-03-13:
Return on 2026-03-13: (1200/83884.24)*70544.43 - 1200 = -190 USD

Case 2: Invested $100/month over 12 months from 2025-03-13 to 2026-02-13:
Return as of 2026-03-13:  -325 USD 
```sql 
select sum(100/close_price) as total_bitcoin_units,
 sum(100/close_price)*70544.43 - 1200 as `return`
from dbt_marts.fact_daily_prices
where symbol = 'bitcoin' and extract(day from price_date) = 13
and price_date <= '2026-02-13'
```
Both cases last year from 2025-03-13 made a **loss** but an initial lump sum investment made lesser loss compared to monthly investment

### Volatility comparison: fiat currencies vs Bitcoin
Looking purely at returns, bitcoin is more volatile. We can look at volatility metrics to see if daily return of bitcoin vs other stocks/forex and index is more volatile.
```sql
select symbol, relative_volatility_365d_vs_btc
from dbt_marts.asset_performance_summary_latest 
```
| Symbol    | Volatility vs BTC (365d) | Interpretation               |
|-----------|-------------------------:|------------------------------|
| C:GBPUSD  |                   0.1763 | 17.6% of BTC's volatility    |
| C:EURUSD  |                   0.1889 | 18.9% of BTC's volatility    |
| SPY       |                   0.5140 | 51.4% of BTC's volatility    |
| MSFT      |                   0.7115 | 71.2% of BTC's volatility    |
| GOOGL     |                   0.8288 | 82.9% of BTC's volatility    |
| AAPL      |                   0.8682 | 86.8% of BTC's volatility    |


This query shows that all assets are less volatile than bitcoin. Forex pairs are the most stable while stocks are closest to bitcoin. SPY is half as volatile as bitcoin. 

## Extra credit

### Swap events
1. Please see the attached `etherscan_swap_events.pdf` in `sample_data/swap_events` for recent swap events
2. For the provided swap event in the screenshot:
sender:0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D
to: 0x423D607Bd4E213e9b64a54b324Ab7F632FEeC647
amount0In (uint256) :0
amount1In (uint256) : 45493335506378983
amount0Out (uint256): 101969968
amount1Out (uint256) : 0
3. For the provided swap event in the provided screenshot at `swap_events/single_swap_event.png`:
topic0: 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
transaction hash: 0x547195a1b9b65fc73f8c71d20cea0c6f5c8d7f6e021904471a2b988980ec92ff
address: 0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc
token0:0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 
token1:0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 
4. Human-readable values are:
token0: USDC
token1: WETH

### Dune Analytics
[Query] (https://dune.com/queries/5728529/9297690)