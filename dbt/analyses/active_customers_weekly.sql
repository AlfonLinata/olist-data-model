select 
    distinct date_trunc(date(order_purchase_timestamp), week) as snapshot_week,
    customers.customer_unique_id
    
from {{ ref('fact__order_items') }}
