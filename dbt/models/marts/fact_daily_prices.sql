{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Fact Table: fact_daily_prices
  One row per asset per trading day.
  Combines Massive (stocks/forex/indices) and CoinGecko (crypto) staging data.
  LAG() and return calculations are done per source before combining.

  TODO: Convert to incremental merge on (symbol, price_date) for scalability.
  Historical returns are static — only new data needs LAG() calculation.
  Approach: fetch last existing row per symbol as anchor for LAG() boundary,
  union with new staging rows, compute returns, merge only new rows back.
  Partition by price_date, cluster by symbol. Need to take care with late-arriving data and potential updates to historical prices if it ever happens (e.g. from data corrections in staging).
*/

WITH massive_with_returns AS (
    SELECT
        symbol,
        price_date,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        CAST(NULL AS FLOAT64) AS market_cap,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_price,
        LAG(price_date)  OVER (PARTITION BY symbol ORDER BY price_date) AS prev_price_date,
        __de_processed_at,
        __run_id
    FROM {{ ref('stg_massive_prices') }}
),

coingecko_with_returns AS (
    SELECT
        symbol,
        price_date,
        close_price,
        CAST(NULL AS FLOAT64) AS open_price,
        CAST(NULL AS FLOAT64) AS high_price,
        CAST(NULL AS FLOAT64) AS low_price,
        volume,
        market_cap,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_price,
        LAG(price_date)  OVER (PARTITION BY symbol ORDER BY price_date) AS prev_price_date,
        __de_processed_at,
        __run_id
    FROM {{ ref('stg_coingecko_prices') }}
),

combined AS (
    SELECT * FROM massive_with_returns
    UNION ALL
    SELECT * FROM coingecko_with_returns
),

with_calculations AS (
    SELECT
        *,
        CASE
            WHEN prev_close_price IS NOT NULL AND prev_close_price > 0
            THEN (close_price - prev_close_price) / prev_close_price
        END AS daily_return,

        CASE
            WHEN prev_close_price IS NOT NULL AND prev_close_price > 0 AND close_price > 0
            THEN LN(close_price / prev_close_price)
        END AS log_return,

        CASE
            WHEN high_price IS NOT NULL AND low_price IS NOT NULL AND close_price > 0
            THEN (high_price - low_price) / close_price
        END AS intraday_range_pct,

        CASE
            WHEN prev_price_date IS NOT NULL
            THEN DATE_DIFF(price_date, prev_price_date, DAY)
        END AS days_since_prev

    FROM combined
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['wc.symbol', 'wc.price_date']) }} AS price_key,

    da.symbol          AS asset_key,
    dd.date_key,

    wc.symbol,
    da.asset_type,
    wc.price_date,

    wc.close_price,
    wc.open_price,
    wc.high_price,
    wc.low_price,
    wc.volume,
    wc.market_cap,

    wc.daily_return,
    wc.log_return,
    wc.prev_close_price,
    wc.intraday_range_pct,
    wc.days_since_prev,

    CASE WHEN wc.days_since_prev > 5 THEN TRUE ELSE FALSE END AS has_data_gap,

    wc.__run_id,
    wc.__de_processed_at,
    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM with_calculations wc
LEFT JOIN {{ ref('dim_assets') }} da ON da.symbol = wc.symbol
LEFT JOIN {{ ref('dim_dates') }}  dd ON dd.date_key = wc.price_date
