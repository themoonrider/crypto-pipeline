{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  Dimension: dim_dates
  Full calendar date spine covering the analysis window + buffer.
*/

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "(SELECT MIN(first_seen) FROM " ~ ref('dim_assets') ~ ")",
        end_date   = "(SELECT DATE_ADD(MAX(last_seen), INTERVAL 1 DAY) FROM " ~ ref('dim_assets') ~ ")"
    ) }}
),

calendar AS (
    SELECT
        CAST(date_day AS DATE)                                      AS date_key,
        CAST(date_day AS DATE)                                      AS full_date,

        -- Calendar parts
        EXTRACT(YEAR  FROM date_day)                                AS year,
        EXTRACT(MONTH FROM date_day)                                AS month,
        EXTRACT(DAY   FROM date_day)                                AS day,
        EXTRACT(DAYOFWEEK FROM date_day)                            AS day_of_week,   -- 1=Sun in BQ
        EXTRACT(DAYOFYEAR FROM date_day)                            AS day_of_year,
        EXTRACT(ISOWEEK FROM date_day)                              AS week_of_year,
        EXTRACT(QUARTER FROM date_day)                              AS quarter,

        -- Labels
        FORMAT_DATE('%A', CAST(date_day AS DATE))                   AS day_name,
        FORMAT_DATE('%B', CAST(date_day AS DATE))                   AS month_name,
        FORMAT_DATE('%Y-%m', CAST(date_day AS DATE))                AS year_month,
        CONCAT(CAST(EXTRACT(YEAR FROM date_day) AS STRING), '-Q',
               CAST(EXTRACT(QUARTER FROM date_day) AS STRING))     AS year_quarter,

        -- Flags (1=Sun, 7=Sat in BigQuery)
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)
             THEN TRUE ELSE FALSE END                               AS is_weekend,
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) NOT IN (1, 7)
             THEN TRUE ELSE FALSE END                               AS is_weekday,

        -- Relative
        DATE_DIFF(CURRENT_DATE(), CAST(date_day AS DATE), DAY)     AS days_ago,
        CAST(date_day AS DATE) = CURRENT_DATE()                    AS is_today

    FROM date_spine
)

SELECT * FROM calendar
