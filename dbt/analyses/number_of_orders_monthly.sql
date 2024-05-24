select 
    date_trunc(order_purchase_timestamp, month) as order_purchase_monthly,
    count(*) as number_of_orders
    
from {{ ref('fact__order_items') }}
group by 1
order by 1