-- stg_taxi_trips.sql
-- Staging model for NYC Yellow Taxi trips
-- Cleans and standardizes raw taxi data from the staging layer
-- Materialized as a view — no storage cost, always fresh

with source as (
    select * from {{ source('staging', 'taxi_weather_staging') }}
),

cleaned as (
    select
        -- Trip identifiers
        vendor_id,
        cast(pickup_location_id as string)  as pickup_location_id,
        cast(dropoff_location_id as string) as dropoff_location_id,

        -- Timestamps
        cast(pickup_datetime as timestamp)  as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(trip_date as date)             as trip_date,

        -- Trip metrics
        cast(passenger_count as integer)    as passenger_count,
        cast(trip_distance as numeric)      as trip_distance,
        cast(trip_duration_minutes as integer) as trip_duration_minutes,
        cast(pickup_hour as integer)        as pickup_hour,
        cast(day_of_week as integer)        as day_of_week,

        -- Financial metrics
        cast(fare_amount as numeric)        as fare_amount,
        cast(tip_amount as numeric)         as tip_amount,
        cast(total_amount as numeric)       as total_amount,
        cast(tolls_amount as numeric)       as tolls_amount,
        cast(mta_tax as numeric)            as mta_tax,
        cast(extra as numeric)              as extra,
        cast(imp_surcharge as numeric)      as imp_surcharge,
        coalesce(cast(airport_fee as numeric), 0) as airport_fee,

        -- Derived columns
        cast(fare_per_mile as numeric)      as fare_per_mile,
        cast(tip_percentage as numeric)     as tip_percentage,
        cast(is_airport_trip as boolean)    as is_airport_trip,
        trip_category,
        payment_type,
        rate_code,

        -- Weather at time of trip
        cast(temperature_f as numeric)      as temperature_f,
        cast(humidity_pct as numeric)       as humidity_pct,
        cast(precipitation_inch as numeric) as precipitation_inch,
        cast(wind_speed_mph as numeric)     as wind_speed_mph,
        weather_condition,
        cast(is_raining as boolean)         as is_raining,
        cast(is_cold as boolean)            as is_cold,
        temp_category,

        -- Metadata
        ingestion_date,
        source_system,
        source_table

    from source
    where
        fare_amount > 0
        and trip_distance > 0
        and passenger_count > 0
        and trip_duration_minutes > 0
        and trip_duration_minutes < 300
        and total_amount > 0
)

select * from cleaned