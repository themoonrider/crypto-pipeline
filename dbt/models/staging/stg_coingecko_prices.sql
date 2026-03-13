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
  Staging model for CoinGecko market chart data.
  - Parses raw_json: flattens prices, market_caps, total_volumes in one pass via UNNEST WITH OFFSET
  - Dedupes on (coin_id, price_date), keeping the latest ingestion
  - Filters out rows with null/zero prices
  - Incremental: only processes new raw rows since last run
*/

WITH source AS (
    SELECT
        coin_id,
        vs_currency,
        raw_json,
        __de_processed_at,
        __run_id
    FROM {{ source('raw_market_data', 'coingecko_market_chart') }}
    {% if is_incremental() %}
    WHERE __de_processed_at > (SELECT TIMESTAMP_SUB(MAX(__de_processed_at), INTERVAL 3 DAY) FROM {{ this }})
    {% endif %}
),

flattened AS (
    SELECT
        s.coin_id,
        s.vs_currency,
        DATE(TIMESTAMP_MILLIS(CAST(CAST(JSON_VALUE(p, '$[0]') AS FLOAT64) AS INT64))) AS price_date,
        CAST(JSON_VALUE(p, '$[1]') AS FLOAT64)                      AS close_price,
        CAST(JSON_VALUE(v, '$[1]') AS FLOAT64)                      AS volume,
        CAST(JSON_VALUE(mc, '$[1]') AS FLOAT64)                     AS market_cap,
        s.__de_processed_at,
        s.__run_id
    FROM source s,
    UNNEST(JSON_EXTRACT_ARRAY(s.raw_json, '$.prices'))        AS p WITH OFFSET AS idx_p,
    UNNEST(JSON_EXTRACT_ARRAY(s.raw_json, '$.total_volumes')) AS v WITH OFFSET AS idx_v,
    UNNEST(JSON_EXTRACT_ARRAY(s.raw_json, '$.market_caps'))   AS mc WITH OFFSET AS idx_mc
    WHERE idx_p = idx_v AND idx_p = idx_mc
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, price_date
            ORDER BY __de_processed_at DESC
        ) AS row_num
    FROM flattened
    WHERE close_price IS NOT NULL AND close_price > 0
)

SELECT
    coin_id as symbol,
    vs_currency,
    price_date,
    close_price,
    volume,
    market_cap,
    __de_processed_at,
    __run_id
FROM ranked
WHERE row_num = 1
