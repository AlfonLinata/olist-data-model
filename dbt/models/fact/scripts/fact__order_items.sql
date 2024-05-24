{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "last_updated_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

-- with insert overwrite strategy this table will be duplicated with the same order id,
-- but different order status
with olist_orders_dataset as (

    select * from {{ source('raw', 'olist_orders_dataset') }}

),

olist_order_items_dataset as (

    select * from {{ source('raw', 'olist_order_items_dataset') }}

),

dim__products as (

    select * from {{ ref('dim__products') }}

),

dim__customers as (

    select * from {{ ref('dim__customers') }}

),

items as (

    select 
        item.order_id,
        array_agg(
            struct(
                item.order_item_id, 
                struct(
                    prod.dbt_scd_id as product_scd_id,
                    prod.product_id, 
                    prod.product_category_name,
                    prod.product_category_name_english,
                    prod.product_name_lenght, 
                    prod.product_description_lenght,
                    prod.product_photos_qty,
                    prod.product_weight_g, 
                    prod.product_length_cm, 
                    prod.product_height_cm, 
                    prod.product_width_cm
                ) as product,
                item.seller_id, 
                item.shipping_limit_date, 
                item.price, 
                item.freight_value
            )
        ) as items

    from olist_order_items_dataset item
    left join dim__products prod
        on item.product_id = prod.product_id
    where dbt_valid_to is null
    group by 1

),

customer as (

    select 
        dbt_scd_id as customer_scd_id,
        customer_id, 
        customer_unique_id, 
        customer_zip_code_prefix, 
        customer_city, 
        customer_state

    from dim__customers
    where dbt_valid_to is null

),

final as (

    select 
        orders.order_id, 
        orders.order_status, 
        orders.order_purchase_timestamp, 
        orders.order_approved_at as order_approved_timestamp, 
        orders.order_delivered_carrier_date as order_delivered_carrier_timestamp, 
        orders.order_delivered_customer_date as order_delivered_customer_timestamp, 
        orders.order_estimated_delivery_date as order_estimated_delivery_timestamp,
        greatest(
            coalesce(orders.order_purchase_timestamp, '1900-01-01'), 
            coalesce(orders.order_approved_at, '1900-01-01'), 
            coalesce(orders.order_delivered_carrier_date, '1900-01-01'), 
            coalesce(orders.order_delivered_customer_date, '1900-01-01')
            ) as last_updated_timestamp, 
        struct(
            cust.customer_scd_id,
            cust.customer_id, 
            cust.customer_unique_id, 
            cust.customer_zip_code_prefix, 
            cust.customer_city, 
            cust.customer_state
        ) as customers,
        items.items


    from olist_orders_dataset orders
    left join items
        on orders.order_id = items.order_id
    left join customer as cust
        on cust.customer_id = orders.customer_id

    {% if is_incremental() -%}

    -- This filter will only be applied on an incremental run
    -- These columns is the rough estimate of last_updated_timestamp
    -- Some status still won't be captured, like canceled.
    -- To address this problem, we should change the source schema 
    -- or not using incremental changes run
    where date(greatest(
        coalesce(orders.order_purchase_timestamp, '1900-01-01'), 
        coalesce(orders.order_approved_at, '1900-01-01'), 
        coalesce(orders.order_delivered_carrier_date, '1900-01-01'), 
        coalesce(orders.order_delivered_customer_date, '1900-01-01')
    )) = current_date() - 1

    {%- endif -%}

)

select * from final

