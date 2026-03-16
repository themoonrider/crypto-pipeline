{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Mart: asset_performance_summary
  One row per asset. Snapshot of key performance metrics.
  Best/worst performing assets vs Bitcoin, volatility ranking.
*/

WITH latest_dates AS (
    SELECT symbol, MAX(price_date) AS latest_date
    FROM {{ ref('rolling_performance') }}
    GROUP BY symbol
),

latest AS (
    SELECT
        rp.symbol,
        rp.asset_type,
        rp.price_date          AS latest_date,
        rp.close_price         AS latest_price,
        rp.daily_return,
        rp.return_7d,
        rp.return_30d,
        rp.return_90d,
        rp.return_180d,
        rp.return_365d,
        rp.volatility_7d,
        rp.volatility_30d,
        rp.volatility_90d,
        rp.volatility_180d,
        rp.volatility_365d
    FROM {{ ref('rolling_performance') }} rp
    JOIN latest_dates ld ON rp.symbol = ld.symbol AND rp.price_date = ld.latest_date
),

btc_ref AS (
    SELECT
        return_7d       AS btc_return_7d,
        return_30d      AS btc_return_30d,
        return_90d      AS btc_return_90d,
        return_180d     AS btc_return_180d,
        return_365d     AS btc_return_365d,
        volatility_365d AS btc_vol_365d
    FROM latest
    WHERE symbol = 'bitcoin'
),

summary AS (
    SELECT
        l.*,
        b.btc_return_7d,
        b.btc_return_30d,
        b.btc_return_90d,
        b.btc_return_180d,
        b.btc_return_365d,
        b.btc_vol_365d,

        -- Outperformance vs BTC
        l.return_7d   - b.btc_return_7d    AS vs_btc_7d,
        l.return_30d  - b.btc_return_30d   AS vs_btc_30d,
        l.return_90d  - b.btc_return_90d   AS vs_btc_90d,
        l.return_180d - b.btc_return_180d  AS vs_btc_180d,
        l.return_365d - b.btc_return_365d  AS vs_btc_365d,

        -- Relative volatility
        CASE WHEN b.btc_vol_365d > 0
             THEN l.volatility_365d / b.btc_vol_365d END           AS vol_ratio_vs_btc

    FROM latest l
    CROSS JOIN btc_ref b
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY return_365d DESC)       AS rank_return_365d,
        RANK() OVER (ORDER BY volatility_365d ASC NULLS LAST)    AS rank_low_volatility
    FROM summary
)

SELECT
    symbol,
    asset_type,
    latest_date,
    ROUND(latest_price, 4)               AS latest_price,

    -- Returns
    ROUND(daily_return, 6)               AS daily_return_latest,
    ROUND(return_7d, 6)                  AS return_7d,
    ROUND(return_30d, 6)                 AS return_30d,
    ROUND(return_90d, 6)                 AS return_90d,
    ROUND(return_180d, 6)                AS return_180d,
    ROUND(return_365d, 6)                AS return_365d,

    -- vs Bitcoin
    ROUND(btc_return_365d, 6)            AS btc_return_365d,
    ROUND(vs_btc_7d, 6)                  AS outperformance_vs_btc_7d,
    ROUND(vs_btc_30d, 6)                 AS outperformance_vs_btc_30d,
    ROUND(vs_btc_90d, 6)                 AS outperformance_vs_btc_90d,
    ROUND(vs_btc_180d, 6)                AS outperformance_vs_btc_180d,
    ROUND(vs_btc_365d, 6)                AS outperformance_vs_btc_365d,

    -- Risk
    ROUND(volatility_30d, 6)             AS volatility_30d,
    ROUND(volatility_365d, 6)            AS volatility_365d,
    ROUND(btc_vol_365d, 6)               AS btc_volatility_365d,
    ROUND(vol_ratio_vs_btc, 4)           AS vol_ratio_vs_btc,

    -- Rankings
    rank_return_365d,
    rank_low_volatility,

    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM ranked
ORDER BY rank_return_365d
