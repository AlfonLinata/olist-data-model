with sum_order as (

    select 
        date(order_purchase_timestamp) as order_purchase_date,
        customers.customer_unique_id,
        (select sum(i.price) from unnest(items) as i) as total_price_per_order
        
    from {{ ref('fact__order_items') }}

)

select
    order_purchase_date,
    customer_unique_id,
    avg(total_price_per_order) as average_price_per_customer_per_day

from sum_order
group by 1,2

