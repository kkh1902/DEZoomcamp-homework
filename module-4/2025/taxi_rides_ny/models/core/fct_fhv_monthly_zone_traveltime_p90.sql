{{ config(materialized='table') }}

WITH trip_duration AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        pickup_location_id,
        dropoff_location_id,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_seconds
    FROM {{ ref('stg_fhv_tripdata') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND EXTRACT(MONTH FROM pickup_datetime) = 11
        AND pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND dropoff_datetime > pickup_datetime
),
p90 AS (
    SELECT DISTINCT
        year, month, pickup_location_id, dropoff_location_id,
        PERCENTILE_CONT(trip_seconds, 0.9)
            OVER (PARTITION BY year, month, pickup_location_id, dropoff_location_id) AS p90_trip_duration
    FROM trip_duration
),
zone_p90 AS (
    SELECT
        p90.year,
        p90.month,
        p90.pickup_location_id,
        p90.dropoff_location_id,
        TRIM(z_pu.zone) AS pickup_zone,
        TRIM(z_do.zone) AS dropoff_zone,
        p90.p90_trip_duration
    FROM p90
    INNER JOIN {{ ref('dim_zones') }} z_pu
        ON p90.pickup_location_id = z_pu.locationid
    INNER JOIN {{ ref('dim_zones') }} z_do
        ON p90.dropoff_location_id = z_do.locationid
    WHERE TRIM(z_pu.zone) IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND z_do.borough IS NOT NULL
        AND TRIM(z_do.zone) NOT IN ('NV', 'Unknown')
),
ranked_trips AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) AS rank
    FROM zone_p90
)
SELECT pickup_zone, dropoff_zone, p90_trip_duration
FROM ranked_trips
WHERE rank = 2
ORDER BY pickup_zone
