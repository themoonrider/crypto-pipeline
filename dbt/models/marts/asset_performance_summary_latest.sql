{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Mart: asset_performance_summary on latest available date of data. 
  One row per asset. Snapshot of key performance metrics.
  Best/worst performing assets vs Bitcoin, volatility ranking.
*/

WITH latest_dates AS (
    SELECT MAX(price_date) AS latest_date
    FROM {{ ref('btc_relative_performance') }}
)
SELECT 
    *except(price_date, __dbt_updated_at),
    price_date AS latest_price_date,
    CURRENT_TIMESTAMP() AS __dbt_updated_at

FROM {{ ref('btc_relative_performance') }} rp
JOIN latest_dates ld ON rp.price_date = ld.latest_date
