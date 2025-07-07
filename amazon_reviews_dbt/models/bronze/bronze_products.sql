{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

select asin as product_id,
    title as product_name,
    description as product_description,
    brand,
    cast(price as float64) as price_usd,
    imUrl as image_url,
    categories as categories_json,
    salesRank as sales_rank_json,
    related as related_products_json,
    current_timestamp() as _loaded_at
from {{source("landing_zone","metadata")}}