{{ config(materialized='table') }}

WITH base AS (
    SELECT
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
),

p90_calc AS (
    SELECT
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90)
            OVER (
                PARTITION BY
                    year,
                    month,
                    pickup_location_id,
                    dropoff_location_id
            ) AS p90_trip_duration
    FROM base
)

SELECT *
FROM p90_calc
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        year,
        month,
        pickup_location_id,
        dropoff_location_id
) = 1
