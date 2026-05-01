-- dim_date.sql
-- Date dimension table covering full year 2022
-- Uses dbt_utils.date_spine to generate all dates

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2022-12-31' as date)"
    ) }}
),

final AS (
    SELECT
        FORMAT_DATE('%Y%m%d', date_day)         AS date_key,
        date_day                                 AS full_date,
        EXTRACT(YEAR FROM date_day)              AS year,
        EXTRACT(MONTH FROM date_day)             AS month,
        EXTRACT(DAY FROM date_day)               AS day,
        EXTRACT(QUARTER FROM date_day)           AS quarter,
        EXTRACT(DAYOFWEEK FROM date_day)         AS day_of_week,
        FORMAT_DATE('%A', date_day)              AS day_name,
        FORMAT_DATE('%B', date_day)              AS month_name,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                      AS is_weekend
    FROM date_spine
)

SELECT * FROM final