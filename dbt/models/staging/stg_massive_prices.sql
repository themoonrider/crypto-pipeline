{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['symbol', 'price_date'],
    partition_by = {'field': 'price_date', 'data_type': 'date'},
    cluster_by = ['symbol'],
    schema = 'staging'
  )
}}

/*
  Staging model for Massive API data.
  - Unnests the results array into one row per symbol per date
  - Converts millisecond timestamp to DATE
  - Dedupes on (symbol, price_date), keeping the latest ingestion
  - Filters out non-trading days (all OHLCV null/zero)
  - Incremental: only processes new raw rows since last run
*/

WITH source AS (
    SELECT
        symbol,
        TIMESTAMP_MILLIS(r.timestamp)       AS price_timestamp,
        DATE(TIMESTAMP_MILLIS(r.timestamp))  AS price_date,
        r.open                               AS open_price,
        r.high                               AS high_price,
        r.low                                AS low_price,
        r.close                              AS close_price,
        r.volume                             AS volume,
        __de_processed_at,
        __run_id
    FROM {{ source('raw_market_data', 'massive_prices') }},
    UNNEST(results) AS r
    {% if is_incremental() %}
    WHERE __de_processed_at > (SELECT TIMESTAMP_SUB(MAX(__de_processed_at), INTERVAL 3 DAY) FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, price_date
            ORDER BY __de_processed_at DESC
        ) AS row_num
    FROM source
    WHERE NOT (
        COALESCE(open_price, 0) = 0
        AND COALESCE(high_price, 0) = 0
        AND COALESCE(low_price, 0) = 0
        AND COALESCE(close_price, 0) = 0
        AND COALESCE(volume, 0) = 0
    )
)

SELECT
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    __de_processed_at,
    __run_id
FROM ranked
WHERE row_num = 1
