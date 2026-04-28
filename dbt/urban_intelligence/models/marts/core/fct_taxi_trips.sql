-- fct_taxi_trips.sql
-- Fact table for NYC taxi trips
-- Central table in our star schema
-- Contains all measurable facts about each trip

with stg_trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

final as (
    select
-- Surrogate key -- adding more fields to ensure uniqueness
{{ dbt_utils.generate_surrogate_key([
    'vendor_id',
    'pickup_datetime',
    'pickup_location_id',
    'dropoff_location_id',
    'fare_amount'
]) }} as trip_key,

        -- Foreign keys for dimension tables
        pickup_location_id,
        dropoff_location_id,
        cast(format_date('%Y%m%d', trip_date) as integer) as date_key,

        -- Trip details
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        trip_date,
        pickup_hour,
        day_of_week,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        trip_category,
        payment_type,
        rate_code,
        is_airport_trip,

        -- Financial facts
        fare_amount,
        tip_amount,
        total_amount,
        tolls_amount,
        mta_tax,
        extra,
        imp_surcharge,
        airport_fee,
        fare_per_mile,
        tip_percentage,

        -- Weather facts at time of trip
        temperature_f,
        humidity_pct,
        precipitation_inch,
        wind_speed_mph,
        weather_condition,
        is_raining,
        is_cold,
        temp_category,

        -- Metadata
        ingestion_date,
        source_system

    from stg_trips
)

select * from final