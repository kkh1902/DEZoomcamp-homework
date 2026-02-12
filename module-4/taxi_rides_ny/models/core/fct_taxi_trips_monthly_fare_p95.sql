{{ config(
    materialized = 'table'
) }}

WITH valid_trips AS (

    SELECT
        'yellow' AS service_type,
        EXTRACT(YEAR  FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE
        service_type = 'Yellow'
        AND fare_amount > 0
        AND trip_distance > 0
        AND payment_type IN (1, 2)

    UNION ALL

    SELECT
        'green' AS service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE
        service_type = 'Green'
        AND fare_amount > 0
        AND trip_distance > 0
        AND payment_type IN (1, 2)
),
percentiles AS(
    SELECT 
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97)
            OVER (PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95)
            OVER (PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90)
            OVER (PARTITION BY service_type, year, month) AS p90
    FROM valid_trips 
)

SELECT *
FROM percentiles 
WHERE year = 2020 and month =4