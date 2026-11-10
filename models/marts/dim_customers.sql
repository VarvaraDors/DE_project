{{ config(distributed_replicated=true) }}

with customers as (
    select * from {{ ref('stg_shop__customers') }}
),

confirmed as (
    select 
        customer_id,
        order_id,
        ordered_at,
        order_amount,
        row_number() over (partition by customer_id order by ordered_at asc) as num
    from {{ ref('fct_orders') }}
    where is_confirmed = true
),

customer_conf as (
    select 
        customer_id,
        count(order_id) as confirmed_orders_count,
        sum(order_amount) as confirmed_revenue,
        max(case when num = 1 then order_id end) as first_confirmed_order_id,
        max(case when num = 1 then ordered_at end) as first_confirmed_at
    from confirmed
    group by customer_id
)

select 
    c.customer_id,
    c.registration_date, 
    cc.first_confirmed_order_id,
    cc.first_confirmed_at,
    coalesce(cc.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(cc.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(cc.confirmed_orders_count, 0) > 0 as is_buyer
from customers c
left join customer_conf cc on c.customer_id = cc.customer_id