{{ config(distributed_replicated=true) }}

with recursive category_tree as (
    select 
        category_id,
        category_name,
        parent_category_id,
        category_name as root_category_name,
        1 as lvl
    from {{ ref('stg_shop__product_categories') }}
    where parent_category_id is null

    union all

    select 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        ct.root_category_name,
        ct.lvl + 1
    from {{ ref('stg_shop__product_categories') }} c
    join category_tree ct on c.parent_category_id = ct.category_id
),

products_with_categories as (
    select 
        p.product_id,
        p.product_name,
        p.brand,
        ct.category_name,
        pc.category_name as parent_category_name,
        ct.root_category_name
    from {{ ref('stg_shop__products') }} p
    left join category_tree ct on p.category_name = ct.category_name
    left join {{ ref('stg_shop__product_categories') }} pc 
        on ct.parent_category_id = pc.category_id
)

select 
    product_id,
    product_name,
    brand,
    category_name,
    parent_category_name,
    root_category_name
from products_with_categories