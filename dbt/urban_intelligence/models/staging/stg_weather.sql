-- stg_weather.sql
-- Staging model for NYC weather data
-- Source: raw.weather_raw (loaded from Open-Meteo API in Day 2)

WITH source AS (
    SELECT * FROM {{ source('raw', 'weather_raw') }}
),

cleaned AS (
    SELECT
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['date', 'hour']) }} AS weather_key,

        -- Time dimensions
        CAST(date AS DATE)                             AS weather_date,
        CAST(hour AS INT64)                            AS weather_hour,

        -- Temperature
        ROUND(CAST(temperature_f AS FLOAT64), 2)      AS temperature_f,

        -- Precipitation and humidity
        ROUND(CAST(precipitation_inch AS FLOAT64), 3) AS precipitation_inch,
        ROUND(CAST(humidity_pct AS FLOAT64), 2)       AS humidity_pct,

        -- Wind
        ROUND(CAST(wind_speed_mph AS FLOAT64), 2)     AS wind_speed_mph,

        -- Weather classification
        CAST(weather_code AS INT64)                    AS weather_code,
        CAST(weather_condition AS STRING)              AS weather_condition,

        -- Metadata
        CAST(ingestion_date AS DATE)                   AS ingestion_date

    FROM source
    WHERE
        date IS NOT NULL
        AND hour IS NOT NULL
)

SELECT * FROM cleaned