{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Mart: rolling_performance
  Computes rolling window performance metrics for all assets.
  Windows: 7, 30, 90, 180, 365 days.

  For each asset and date:
    - Rolling return (cumulative price change)
    - Rolling volatility (annualised std dev of daily returns)
*/

WITH base AS (
    SELECT
        symbol,
        asset_type,
        price_date,
        close_price,
        daily_return,
        log_return
    FROM {{ ref('fact_daily_prices') }}
    WHERE close_price IS NOT NULL
),

rolling AS (
    SELECT
        symbol,
        asset_type,
        price_date,
        close_price,
        daily_return,

        -- ── 7-day window ───────────────────────────────────────────────────
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                                           AS price_7d_ago,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) * SQRT(252)                                               AS volatility_7d,

        COUNT(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                                           AS obs_7d,

        -- ── 30-day window ──────────────────────────────────────────────────
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                                           AS price_30d_ago,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * SQRT(252)                                               AS volatility_30d,

        COUNT(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                                           AS obs_30d,

        -- ── 90-day window ──────────────────────────────────────────────────
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )                                                           AS price_90d_ago,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) * SQRT(252)                                               AS volatility_90d,

        COUNT(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        )                                                           AS obs_90d,

        -- ── 180-day window ─────────────────────────────────────────────────
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
        )                                                           AS price_180d_ago,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
        ) * SQRT(252)                                               AS volatility_180d,

        COUNT(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
        )                                                           AS obs_180d,

        -- ── 365-day window ─────────────────────────────────────────────────
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        )                                                           AS price_365d_ago,

        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) * SQRT(252)                                               AS volatility_365d,

        COUNT(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        )                                                           AS obs_365d

    FROM base
),

with_metrics AS (
    SELECT
        *,
        -- Rolling returns (simple, not annualised)
        CASE WHEN price_7d_ago   > 0 THEN (close_price - price_7d_ago)   / price_7d_ago   END AS return_7d,
        CASE WHEN price_30d_ago  > 0 THEN (close_price - price_30d_ago)  / price_30d_ago  END AS return_30d,
        CASE WHEN price_90d_ago  > 0 THEN (close_price - price_90d_ago)  / price_90d_ago  END AS return_90d,
        CASE WHEN price_180d_ago > 0 THEN (close_price - price_180d_ago) / price_180d_ago END AS return_180d,
        CASE WHEN price_365d_ago > 0 THEN (close_price - price_365d_ago) / price_365d_ago END AS return_365d,

    FROM rolling
)

SELECT
    symbol,
    asset_type,
    price_date,
    close_price,
    daily_return,

    -- Returns by window
    ROUND(return_7d,   6)  AS return_7d,
    ROUND(return_30d,  6)  AS return_30d,
    ROUND(return_90d,  6)  AS return_90d,
    ROUND(return_180d, 6)  AS return_180d,
    ROUND(return_365d, 6)  AS return_365d,

    -- Volatility by window (annualised)
    ROUND(volatility_7d,   6)  AS volatility_7d,
    ROUND(volatility_30d,  6)  AS volatility_30d,
    ROUND(volatility_90d,  6)  AS volatility_90d,
    ROUND(volatility_180d, 6)  AS volatility_180d,
    ROUND(volatility_365d, 6)  AS volatility_365d,

    -- Observation counts (data completeness)
    obs_7d, obs_30d, obs_90d, obs_180d, obs_365d,

    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM with_metrics
-- Analysis window anchored to latest ingested data, not CURRENT_DATE()
WHERE price_date >= DATE_SUB(
    (SELECT MAX(price_date) FROM {{ ref('fact_daily_prices') }}),
    INTERVAL {{ var("lookback_days", 365) }} DAY
)
