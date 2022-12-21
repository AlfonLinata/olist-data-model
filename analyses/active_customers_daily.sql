select 
    distinct date(order_purchase_timestamp) as snapshot_date,
    customers.customer_unique_id
    
from {{ ref('fact__order_items') }}