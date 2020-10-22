with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select 
        raw.jaffle_shop.stg_payments.order_id,
        raw.jaffle_shop.stg_payments.amount,
        raw.jaffle_shop.stg_payments.status,
        orders.customer_id
    from raw.jaffle_shop.stg_payments
    left join orders using (order_id)
),

customer_payments as (

    select
        customer_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by 1
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_payments.amount, 0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)
    left join customer_payments using (customer_id)

)

select * from final