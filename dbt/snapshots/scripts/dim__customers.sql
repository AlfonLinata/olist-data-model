{%- snapshot dim__customers -%}


{{
    config(
        target_dataset='dim',
        unique_key='customer_id',
        
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=true,
    )
}}

with olist_customers_dataset as (

    select * from {{ source('raw', 'olist_customers_dataset') }}

),

final as (

    select 
    cust.customer_id, 
    cust.customer_unique_id, 
    cust.customer_zip_code_prefix, 
    cust.customer_city, 
    cust.customer_state

  from olist_customers_dataset cust

)

select * from final

{%- endsnapshot -%}