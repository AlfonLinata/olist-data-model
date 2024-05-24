with fact as (

    select *
    from {{ ref('fact__order_items') }}
    unnest(items) as i

)

select
    a.order_purchase_timestamp,
    b.order_purchase_timestamp as previous_order_purchase_timestamp,
    a.order_id,
    b.order_id as previous_order_id,
    a.customers.customer_unique_id,
    a.product.product_id,
    a.product.product_category_name,
    a.product.product_category_name_english,
    
from fact a
join fact b
    on a.customers.customer_unique_id = b.customers.customer_unique_id
    and a.order_purchase_timestamp > b.order_purchase_timestamp
    and a.product.product_category_name = b.product.product_category_name
