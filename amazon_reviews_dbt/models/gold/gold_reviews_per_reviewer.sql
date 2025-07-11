{{
    config(
        materialized='table',
        schema='gold'
        )
}}

with reviews_per_user as (
select reviewer_id,
    count(review_sk) as review_count
from {{ref('silver_fct_review')}}
group by reviewer_id)
select reviewer_id,
    review_count,
    case when review_count = 1 then '1 review'
    when review_count between 2 and 5 then '2-5 reviews'
    when review_count between 6 and 10 then '6-10 reviews'
    when review_count between 11 and 20 then '11-20 reviews'
    when review_count between 21 and 50 then '21-50 reviews'
    when review_count between 51 and 100 then '51-100 reviews'
    when review_count between 101 and 500 then '101-500 reviews'
    else '500+ reviews'
end as review_bucket,
case when review_count = 1 then 1
    when review_count between 2 and 5 then 2
    when review_count between 6 and 10 then 3
    when review_count between 11 and 20 then 4
    when review_count between 21 and 50 then 5
    when review_count between 51 and 100 then 6
    when review_count between 101 and 500 then 7
    else 8
end as bucket_order,
from reviews_per_user