select 
    date(order_purchase_timestamp) as order_purchase_date,
    customers.customer_unique_id,
    avg(i.price) as average_price_per_user_per_day
    
from {{ ref('fact__order_items') }},
unnest(items) as i
group by 1,2


