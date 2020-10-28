select
    customer_id,
    number_of_orders
from {{ ref('fct_orders') }}
where number_of_orders < 0