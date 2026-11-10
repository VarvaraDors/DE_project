select 
    customer_id,
    registration_date,
    first_confirmed_at
from {{ ref('dim_customers') }}
where first_confirmed_at < registration_date