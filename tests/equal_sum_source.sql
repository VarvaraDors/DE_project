with source_sum as (
    select sum(o.total_amount) as sum_amount
    from {{ source('shop', 'orders') }} o
    join {{ source('shop', 'payments') }} p on o.order_id = p.order_id
    where o.status = 'paid' and p.payment_status = 'success'
),
mart_sum as (
    select sum(confirmed_revenue) as sum_amount
    from {{ ref('dim_customers') }}
)
select 
    source_sum.sum_amount as src_sum,
    mart_sum.sum_amount as mrt_sum
from source_sum, mart_sum 
where source_sum.sum_amount = mart_sum.sum_amount