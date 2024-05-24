{%- snapshot dim__products -%}

{{
    config(
        target_dataset='dim',
        unique_key='product_id',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True,
    )
}}

with olist_products_dataset as (

    select * from {{ source('raw', 'olist_products_dataset') }}

),

product_category_name_translation as (

    select * from {{ source('raw', 'product_category_name_translation') }}

),

final as (

    select
        prod.product_id, 
        prod.product_category_name,
        trans.product_category_name_english,
        prod.product_name_lenght, 
        prod.product_description_lenght,
        prod.product_photos_qty,
        prod.product_weight_g, 
        prod.product_length_cm, 
        prod.product_height_cm, 
        prod.product_width_cm

    from olist_products_dataset as prod
    left join product_category_name_translation trans
        on prod.product_category_name = trans.product_category_name

)

select * from final

{%- endsnapshot -%}