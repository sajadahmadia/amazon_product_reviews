{{
    config(
        materialized='table',
        schema='bronze'
    )
}}

select reviewerID as reviewer_id,
    asin as product_id,
    reviewerName as reviewer_name,
    reviewText as reviewer_text,
    summary as review_summary,
    cast(overall as float64) as rating,
    helpful as helpful_array,
    cast(unixReviewTime as int64) as unix_review_time,
    reviewTime as review_time_raw,
    current_timestamp() as _loaded_at
from {{source("landing_zone", "items_dedup")}}
