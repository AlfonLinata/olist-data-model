with fact as (

    select *
    from {{ ref('fact__order_items') }},
    unnest(items) as i

),

check_category as (

    select
        a.product.product_category_name = b.product.product_category_name as is_same_product_category
        
    from fact a
    join fact b
        on a.customers.customer_unique_id = b.customers.customer_unique_id
        and a.order_purchase_timestamp > b.order_purchase_timestamp

        -- null category won't be included for the calculation
        and a.product.product_category_name is not null
        and b.product.product_category_name is not null

)

select sum(
    if(is_same_product_category = false, 1, 0)
    ) / count(*) * 100 
    as cross_category_retention_percentage
    
from check_category




