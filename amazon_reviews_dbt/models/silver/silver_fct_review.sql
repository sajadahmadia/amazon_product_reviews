{{
    config(
        materialized='incremental',
        schema='silver',
        cluster_by=['product_id', 'reviewer_id'],
        partition_by={
            'field': 'review_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        incremental_strategy='merge' ,
        unique_key='review_sk'
    )
}}

select  
    -- a surrogate key for our fact table
    {{ dbt_utils.generate_surrogate_key(['reviewer_id', 'product_id','unix_review_time']) }} AS review_sk,
    -- natural keys
    reviewer_id,
    product_id,
    cast(format_date('%Y%m%d', date(timestamp_seconds(unix_review_time))) as int64) as date_id,


    date(timestamp_seconds(unix_review_time)) as review_date,
    rating,
    -- assumption: first number in this array shows upvoted likes the review could get
    cast(helpful_array[safe_offset(0)] as int64) as helpful_votes,
    --assumption: second number shows total vots
    cast(helpful_array[safe_offset(1)] as int64) as total_votes,
    review_summary,
    reviewer_text as review_text,
    timestamp_seconds(unix_review_time) as review_timestamp 
from {{ref("bronze_reviews")}}
tablesample SYSTEM (10 PERCENT)

where unix_review_time is not null
    and reviewer_id is not null
    and product_id is not null

{% if is_incremental() %}
    -- a safety check for possible late arriving events (here, reviews) 
    and date(timestamp_seconds(unix_review_time)) >= date_sub(current_date(), interval 3 day)
{% endif %}