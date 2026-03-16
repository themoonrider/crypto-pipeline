/*
  Custom test: data_gap_threshold
  Warns if more than 5% of rows in fact_daily_prices have has_data_gap = TRUE,
  indicating significant missing data in the pipeline.

  Returns failing rows (non-empty = test fails).
*/

WITH gap_stats AS (
    SELECT
        COUNTIF(has_data_gap = TRUE) AS gap_rows,
        COUNT(*)                     AS total_rows
    FROM {{ ref('fact_daily_prices') }}
)

SELECT *
FROM gap_stats
WHERE gap_rows > total_rows * 0.05
