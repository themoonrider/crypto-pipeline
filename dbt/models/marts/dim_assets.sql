{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Dimension: dim_assets
  One row per tradeable asset with descriptive attributes.
  Star schema dimension for the fact_daily_prices fact table.
*/

WITH asset_registry AS (
    -- Static asset metadata (extended with market info)
    SELECT * FROM UNNEST([
        STRUCT('AAPL'   AS symbol, 'stock'  AS asset_type, 'Apple Inc.'               AS asset_name, 'equity' AS asset_class, 'USD' AS currency, 'NASDAQ' AS exchange, 'US'     AS region, FALSE AS is_benchmark),
        STRUCT('GOOGL',  'stock',  'Alphabet Inc.',             'equity',    'USD', 'NASDAQ', 'US',     FALSE),
        STRUCT('MSFT',   'stock',  'Microsoft Corporation',     'equity',    'USD', 'NASDAQ', 'US',     FALSE),
        STRUCT('SPY',    'index',  'S&P 500 ETF (SPDR)',        'index',     'USD', 'NYSE',   'US',     TRUE),
        STRUCT('C:EURUSD', 'forex',  'Euro / US Dollar',          'fx',        'USD', 'FOREX',  'Global', FALSE),
        STRUCT('C:GBPUSD', 'forex',  'British Pound / US Dollar', 'fx',        'USD', 'FOREX',  'Global', FALSE),
        STRUCT('bitcoin',    'crypto', 'Bitcoin',                   'crypto',    'USD', 'Crypto', 'Global', TRUE)
    ])
),

-- Enrich with first/last seen dates from actual data
actual_coverage AS (
    SELECT symbol, MIN(price_date) AS first_seen, MAX(price_date) AS last_seen, COUNT(*) AS trading_days
    FROM {{ ref('stg_massive_prices') }}
    GROUP BY symbol

    UNION ALL

    SELECT symbol, MIN(price_date), MAX(price_date), COUNT(*)
    FROM {{ ref('stg_coingecko_prices') }}
    GROUP BY symbol
)

SELECT
    ar.symbol,
    ar.asset_type,
    ar.asset_name,
    ar.asset_class,
    ar.currency,
    ar.exchange,
    ar.region,
    ar.is_benchmark,

    -- Coverage
    ac.first_seen,
    ac.last_seen,
    COALESCE(ac.trading_days, 0)    AS trading_days_available,

    -- Metadata
    CURRENT_TIMESTAMP()             AS __dbt_updated_at

FROM asset_registry ar
LEFT JOIN actual_coverage ac USING (symbol)
