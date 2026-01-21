{{ config(materialized='view') }}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id
FROM {{ source('staging', 'fhv_tripdata_staging') }}
WHERE dispatching_base_num IS NOT NULL
