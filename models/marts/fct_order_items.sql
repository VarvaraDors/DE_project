{{ config(distributed_by='order_id',orientation='column') }}

with items as (
    select * from {{ ref('stg_shop__order_items') }}
),

orders as (
    select 
        order_id, 
        customer_id, 
        order_date, 
        is_confirmed, 
        is_test_order
    from {{ ref('int_orders_with_payments') }}
)

select
    {{dbt_utils.generate_surrogate_key(['i.order_id', 'i.product_id'])}} as order_item_id,
    i.order_id,
    o.customer_id,
    i.product_id,
    o.order_date,
    i.quantity,
    i.unit_price,
    i.discount_amount,
    i.line_amount,
    o.is_confirmed
from items i
join orders o on i.order_id = o.order_id
where not o.is_test_order