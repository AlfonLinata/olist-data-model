select 
    date_trunc(order_purchase_timestamp, week) as order_purchase_week,
    count(*) as number_of_orders
    
from {{ ref('fact__order_items') }}
group by 1
order by 1