{{
    config(
        materialized = 'table'
    )
}}

with revenue_by_q as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
    group by 1, 2, 3
)

select
    service_type,
    quarter,

    sum(if(year = 2019, revenue, 0)) as revenue_2019,
    sum(if(year = 2020, revenue, 0)) as revenue_2020,

    safe_divide(
        sum(if(year = 2020, revenue, 0)) - sum(if(year = 2019, revenue, 0)),
        sum(if(year = 2019, revenue, 0))
    ) as yoy_growth

from revenue_by_q
group by 1, 2
order by service_type, quarter
