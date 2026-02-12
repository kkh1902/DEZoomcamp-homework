{{
    config(
        materialized = 'table'
    )
}}

with quarter_revenue as (
    SELECT
        'yellow' AS service_type,
        EXTRACT(YEAR FROM pickup_datetime) as year,
        EXTRACT(QUARTER FROM pickup_datetime) as quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q' ,EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SUM(total_amount) as revenue
    FROM {{ ref('fact_trips') }}
    WHERE total_amount> 0
    GROUP BY 1,2,3,4

    UNION ALL

    SELECT
        'green' AS service_type,
        EXTRACT(YEAR FROM pickup_datetime) as year,
        EXTRACT(QUARTER FROM pickup_datetime) as quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q' ,EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SUM(total_amount) as revenue
    FROM {{ ref('fact_trips') }}
    WHERE total_amount> 0
    GROUP BY 1,2,3,4 
),
revenue_with_growth AS (

    SELECT
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.year_quarter,
        q1.revenue as current_revenue,
        q2.revenue as previous_revenue,
        ROUND((q1.revenue-q2.revenue)/ q2.revenue * 100 , 2) AS yoy_growth
    FROM quarter_revenue q1
    LEFT JOIN quarter_revenue q2 
        ON  q1.service_type = q2.service_type
        AND q1.year = q2.year + 1
        AND q1.quarter  = q2.quarter
)

SELECT *
FROM revenue_with_growth
ORDER BY service_type,year,quarter