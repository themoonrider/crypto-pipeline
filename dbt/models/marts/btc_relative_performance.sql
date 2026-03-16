{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Mart: btc_relative_performance
  For each traditional asset, compute performance vs Bitcoin on the same day.

  Metrics:
    - Daily relative return vs BTC
    - Rolling relative performance (7d, 30d, 90d, 180d, 365d)
    - Volatility comparison
*/

WITH btc AS (
    SELECT
        price_date,
        close_price             AS btc_price,
        daily_return            AS btc_daily_return,
        return_7d               AS btc_return_7d,
        return_30d              AS btc_return_30d,
        return_90d              AS btc_return_90d,
        return_180d             AS btc_return_180d,
        return_365d             AS btc_return_365d,
        volatility_7d           AS btc_volatility_7d,
        volatility_30d          AS btc_volatility_30d,
        volatility_90d          AS btc_volatility_90d,
        volatility_180d         AS btc_volatility_180d,
        volatility_365d         AS btc_volatility_365d
    FROM {{ ref('rolling_performance') }}
    WHERE symbol = 'bitcoin'
),

traditional AS (
    SELECT
        rp.symbol,
        rp.asset_type,
        rp.price_date,
        rp.close_price,
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
    WHERE rp.symbol != 'bitcoin'
),

joined AS (
    SELECT
        t.symbol,
        t.asset_type,
        t.price_date,
        t.close_price,
        t.daily_return,

        -- BTC reference
        b.btc_price,
        b.btc_daily_return,

        -- Daily excess return vs BTC
        CASE
            WHEN t.daily_return IS NOT NULL AND b.btc_daily_return IS NOT NULL
            THEN t.daily_return - b.btc_daily_return
        END AS outperformance_1d,

        -- Rolling returns (asset)
        t.return_7d             AS asset_return_7d,
        t.return_30d            AS asset_return_30d,
        t.return_90d            AS asset_return_90d,
        t.return_180d           AS asset_return_180d,
        t.return_365d           AS asset_return_365d,

        -- Rolling returns (BTC)
        b.btc_return_7d,
        b.btc_return_30d,
        b.btc_return_90d,
        b.btc_return_180d,
        b.btc_return_365d,

        -- Rolling outperformance vs BTC (same window comparisons)
        CASE WHEN t.return_7d   IS NOT NULL AND b.btc_return_7d   IS NOT NULL
             THEN t.return_7d   - b.btc_return_7d   END            AS outperformance_7d,
        CASE WHEN t.return_30d  IS NOT NULL AND b.btc_return_30d  IS NOT NULL
             THEN t.return_30d  - b.btc_return_30d  END            AS outperformance_30d,
        CASE WHEN t.return_90d  IS NOT NULL AND b.btc_return_90d  IS NOT NULL
             THEN t.return_90d  - b.btc_return_90d  END            AS outperformance_90d,
        CASE WHEN t.return_180d IS NOT NULL AND b.btc_return_180d IS NOT NULL
             THEN t.return_180d - b.btc_return_180d END            AS outperformance_180d,
        CASE WHEN t.return_365d IS NOT NULL AND b.btc_return_365d IS NOT NULL
             THEN t.return_365d - b.btc_return_365d END            AS outperformance_365d,

        -- Volatility comparison (ratio: asset / BTC), eg: asset is 20% volatile and BTC is 10% volatile over the same window, asset is 2 times more volatile than BTC, ratio = 2.0
        t.volatility_7d     AS asset_volatility_7d,
        b.btc_volatility_7d,
        CASE WHEN b.btc_volatility_7d > 0
             THEN t.volatility_7d / b.btc_volatility_7d END        AS relative_volatility_7d_vs_btc,

        t.volatility_30d    AS asset_volatility_30d,
        b.btc_volatility_30d,
        CASE WHEN b.btc_volatility_30d > 0
             THEN t.volatility_30d / b.btc_volatility_30d END      AS relative_volatility_30d_vs_btc,

        t.volatility_90d    AS asset_volatility_90d,
        b.btc_volatility_90d,
        CASE WHEN b.btc_volatility_90d > 0
             THEN t.volatility_90d / b.btc_volatility_90d END      AS relative_volatility_90d_vs_btc,

        t.volatility_180d   AS asset_volatility_180d,
        b.btc_volatility_180d,
        CASE WHEN b.btc_volatility_180d > 0
             THEN t.volatility_180d / b.btc_volatility_180d END    AS relative_volatility_180d_vs_btc,

        t.volatility_365d   AS asset_volatility_365d,
        b.btc_volatility_365d,
        CASE WHEN b.btc_volatility_365d > 0
             THEN t.volatility_365d / b.btc_volatility_365d END    AS relative_volatility_365d_vs_btc

    FROM traditional t
    JOIN btc b        ON b.price_date = t.price_date
)

SELECT
    symbol,
    asset_type,
    price_date,
    ROUND(close_price, 4)                    AS close_price,
    ROUND(daily_return, 6)                   AS daily_return,
    ROUND(btc_price, 2)                      AS btc_price,
    ROUND(btc_daily_return, 6)               AS btc_daily_return,
    ROUND(outperformance_1d, 6)     AS outperformance_1d,

    ROUND(asset_return_7d, 6)                AS asset_return_7d,
    ROUND(asset_return_30d, 6)               AS asset_return_30d,
    ROUND(asset_return_90d, 6)               AS asset_return_90d,
    ROUND(asset_return_180d, 6)              AS asset_return_180d,
    ROUND(asset_return_365d, 6)              AS asset_return_365d,

    ROUND(btc_return_7d, 6)                  AS btc_return_7d,
    ROUND(btc_return_30d, 6)                 AS btc_return_30d,
    ROUND(btc_return_90d, 6)                 AS btc_return_90d,
    ROUND(btc_return_180d, 6)                AS btc_return_180d,
    ROUND(btc_return_365d, 6)                AS btc_return_365d,

    ROUND(outperformance_7d, 6)              AS outperformance_vs_btc_7d,
    ROUND(outperformance_30d, 6)             AS outperformance_vs_btc_30d,
    ROUND(outperformance_90d, 6)             AS outperformance_vs_btc_90d,
    ROUND(outperformance_180d, 6)            AS outperformance_vs_btc_180d,
    ROUND(outperformance_365d, 6)            AS outperformance_vs_btc_365d,

    ROUND(asset_volatility_7d, 6)             AS asset_volatility_7d,
    ROUND(btc_volatility_7d, 6)              AS btc_volatility_7d,
    ROUND(relative_volatility_7d_vs_btc, 4)  AS relative_volatility_7d_vs_btc,
    ROUND(asset_volatility_30d, 6)           AS asset_volatility_30d,
    ROUND(btc_volatility_30d, 6)             AS btc_volatility_30d,
    ROUND(relative_volatility_30d_vs_btc, 4) AS relative_volatility_30d_vs_btc,
    ROUND(asset_volatility_90d, 6)           AS asset_volatility_90d,
    ROUND(btc_volatility_90d, 6)             AS btc_volatility_90d,
    ROUND(relative_volatility_90d_vs_btc, 4) AS relative_volatility_90d_vs_btc,
    ROUND(asset_volatility_180d, 6)          AS asset_volatility_180d,
    ROUND(btc_volatility_180d, 6)            AS btc_volatility_180d,
    ROUND(relative_volatility_180d_vs_btc, 4) AS relative_volatility_180d_vs_btc,
    ROUND(asset_volatility_365d, 6)          AS asset_volatility_365d,
    ROUND(btc_volatility_365d, 6)            AS btc_volatility_365d,
    ROUND(relative_volatility_365d_vs_btc, 4) AS relative_volatility_365d_vs_btc,

    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM joined
