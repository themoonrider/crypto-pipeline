{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Mart: volatility_correlation_summary
  Full pairwise correlation matrix and volatility comparison.
  One row per asset pair.
*/

WITH daily_returns AS (
    SELECT
        symbol,
        price_date,
        daily_return
    FROM {{ ref('fact_daily_prices') }}
    WHERE
        daily_return IS NOT NULL
        AND price_date >= DATE_SUB(
            (SELECT MAX(price_date) FROM {{ ref('fact_daily_prices') }}),
            INTERVAL {{ var("lookback_days", 365) }} DAY
        )
),

-- Self-join for pairwise correlations
pairs AS (
    SELECT
        a.symbol        AS symbol_a,
        b.symbol        AS symbol_b,
        a.price_date,
        a.daily_return  AS return_a,
        b.daily_return  AS return_b
    FROM daily_returns a
    JOIN daily_returns b ON a.price_date = b.price_date AND a.symbol < b.symbol
),

correlations AS (
    SELECT
        symbol_a,
        symbol_b,
        ROUND(CORR(return_a, return_b), 4)               AS correlation,
        COUNT(*)                                          AS obs_count
    FROM pairs
    GROUP BY symbol_a, symbol_b
),

-- Volatility stats per asset
vol_stats AS (
    SELECT
        symbol,
        ROUND(STDDEV(daily_return) * SQRT(252), 6)               AS annual_volatility,
        ROUND(AVG(daily_return), 6)                               AS avg_daily_return,
        ROUND(MAX(daily_return), 6)                               AS max_daily_gain,
        ROUND(MIN(daily_return), 6)                               AS max_daily_loss,
        ROUND(AVG(daily_return) * 252, 6)                         AS annualized_return,
        COUNT(*)                                                  AS trading_days
    FROM daily_returns
    GROUP BY symbol
)

-- Final output: pairwise correlations joined with both asset stats
SELECT
    c.symbol_a,
    c.symbol_b,
    c.correlation,
    c.obs_count,

    va.annual_volatility    AS volatility_a,
    vb.annual_volatility    AS volatility_b,
    va.annualized_return    AS ann_return_a,
    vb.annualized_return    AS ann_return_b,
    va.avg_daily_return     AS avg_daily_return_a,
    vb.avg_daily_return     AS avg_daily_return_b,
    va.max_daily_gain       AS max_gain_a,
    vb.max_daily_gain       AS max_gain_b,
    va.max_daily_loss       AS max_loss_a,
    vb.max_daily_loss       AS max_loss_b,

    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM correlations c
JOIN vol_stats va ON va.symbol = c.symbol_a
JOIN vol_stats vb ON vb.symbol = c.symbol_b
ORDER BY ABS(c.correlation) DESC
