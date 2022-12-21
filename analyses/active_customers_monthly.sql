select 
    distinct date_trunc(date(order_purchase_timestamp), month) as snapshot_month,
    customers.customer_unique_id
    
from {{ ref('fact__order_items') }}
