with order_sum as (
    select o.order_id, o.order_amount, sum(i.line_amount) as item_sum
    from {{ ref('fct_orders') }} o
    join {{ ref('fct_order_items') }} i on o.order_id = i.order_id
    group by 1, 2
)
select * from order_sum where abs(order_amount - item_sum) > 0.01