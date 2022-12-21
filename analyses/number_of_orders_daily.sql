select 
    date(order_purchase_timestamp) as order_purchase_date,
    count(*) as number_of_orders
    
from {{ ref('fact__order_items') }}
group by 1
order by 1