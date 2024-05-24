with active_customers_weekly as (

    select 
        distinct date_trunc(date(order_purchase_timestamp), week) as snapshot_week,
        customers.customer_unique_id
    
    from {{ ref('fact__order_items') }}

)

select
    snapshot_week,
    customer_unique_id

from active_customers_weekly
qualify
    lag(snapshot_week) over(
        partition by customer_unique_id 
        order by customer_unique_id, snapshot_week
        ) = date_add(snapshot_week, interval -1 week)
order by customer_unique_id, snapshot_week