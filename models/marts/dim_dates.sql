{{ config(distributed_replicated=true) }}
with dates as (
    select 
        min(order_date) as min,
        max(order_date) as max
    from {{ ref('fct_orders') }}
),
date_d as (
    select 
        generate_series(min, max, interval '1 day')::date as date_day
    from dates
)
select 
    date_day,
    extract(year from date_day)::int    as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int   as month,
    extract(isodow from date_day)::int  as day_of_week,
    (extract(isodow from date_day) in (6, 7)) as is_weekend
from date_d