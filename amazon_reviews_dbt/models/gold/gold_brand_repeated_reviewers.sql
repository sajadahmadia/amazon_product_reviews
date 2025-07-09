{{
   config(
       materialized='table',
       schema='gold'
   )
}}

-- here, i tried to find the repeated reviewers per brand, assuming more returning reviewers shows higher brand lo
with brand_review_counts as (
select p.brand,
    count(f.reviewer_id) as total_reviews,
    count(distinct f.reviewer_id) as unique_reviewers,
    round(count(f.reviewer_id) * 1.0 / count(distinct f.reviewer_id), 2) as avg_reviews_per_reviewer
from {{ ref('silver_fct_review') }} f
join {{ ref('silver_dim_products') }} p on f.product_id = p.product_id
where p.brand is not null and trim(p.brand) != ''  
group by p.brand
)
select brand,
   total_reviews,
   unique_reviewers,
   avg_reviews_per_reviewer,
   rank() over (order by avg_reviews_per_reviewer desc) as brand_rank
from brand_review_counts
where unique_reviewers >= 10  -- filter out brands that don't have statistical siginficance
order by avg_reviews_per_reviewer desc