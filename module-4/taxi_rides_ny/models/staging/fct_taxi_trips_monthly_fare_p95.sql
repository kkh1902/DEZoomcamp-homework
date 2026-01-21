{{ config(
    materialized = 'table'
) }}

WITH valid_trips AS (

    SELECT
        service_type,
        EXTRACT(YEAR  FROM pickup_datetime) AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit card')

)

SELECT
    service_type,
    pickup_year,
    pickup_month,

    -- 연속 백분위수
    PERCENTILE_CONT(fare_amount, 0.97)
        OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95)
        OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90)
        OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p90

FROM valid_trips
QUALIFY ROW_NUMBER()
    OVER (PARTITION BY service_type, pickup_year, pickup_month) = 1