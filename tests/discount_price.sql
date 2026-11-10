select * from {{ ref('fct_order_items') }}
where discount_amount > (quantity * unit_price)