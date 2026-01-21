{{ config(materialized='table') }}

WITH fhv AS (
    SELECT
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        EXTRACT(YEAR  FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM {{ ref('stg_fhv_tripdata') }}
)

SELECT
    fhv.*,
    p.zone AS pickup_zone,
    d.zone AS dropoff_zone
FROM fhv
LEFT JOIN {{ ref('dim_zones') }} p
    ON fhv.pickup_location_id = p.locationid
LEFT JOIN {{ ref('dim_zones') }} d
    ON fhv.dropoff_location_id = d.locationid
