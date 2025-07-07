{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

WITH cte AS (
    {% if target.name == 'staging' %}
        SELECT * FROM {{ source('dbt_staging_landing_zone', 'metadata') }}
    {% else %}
        SELECT * FROM {{ source('landing_zone', 'metadata') }}
    {% endif %}
)

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
from cte