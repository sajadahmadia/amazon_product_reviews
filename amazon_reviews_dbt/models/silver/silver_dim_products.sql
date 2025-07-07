{{
    config(
        materialized='incremental',
        schema='silver',
        unique_key='product_id',
        incremental_strategy='merge',
        merge_update_columns=['product_name', 'brand','price_usd',
                            'product_description','primary_category',
                            'detailed_categorization', '_loaded_at']
    )
}}

with cte as (
select *
from {{ref('bronze_products')}}
),
dedup_products as (
select product_id,
    product_name,
    brand,
    price_usd,
    product_description,
    image_url,
    categories_json,

    -- this is my deduplication logic: for any row, 
    -- it gives higher priority to the one that its product categories column is not null, and selectes that in the next step. 
    -- the same goes for other mentioned columns
    row_number() over (
    partition by product_id
    order by case when categories_json is not null then 0 else 1 end,
    case when product_description is not null then 0 else 1 end,
    case when brand is not null then 0 else 1 end,
    case when price_usd is not null then 0 else 1 end,
    case when image_url is not null then 0 else 1 end
  ) as rnk

  from cte
  -- as part of my silver layer (row-wise deletion), i removed products with negative price
  where price_usd >= 0 or price_usd is null
)

select product_id,
    product_name,
    product_description,
    brand,
    price_usd,
    image_url,
    JSON_EXTRACT_SCALAR(categories_json, '$[0][0]') AS primary_category,
    categories_json as detailed_categorization,
    current_timestamp() as _loaded_at
from dedup_products
where rnk = 1 
