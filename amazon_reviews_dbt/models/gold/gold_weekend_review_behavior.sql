-- models/gold/gold_weekend_review_volume.sql
{{
   config(
       materialized='table',
       schema='gold'
   )
}}

with daily_reviews as (
select d.is_weekend,
    count(f.review_sk) as total_reviews,
    count(distinct d.date_day) as total_days,
    round(count(f.review_sk) * 1.0 / count(distinct d.date_day), 2) as avg_reviews_per_day --normalized by available days in our dataset
from {{ ref('silver_fct_review') }} f
join {{ ref('silver_dim_date') }} d on f.date_id = d.date_id
group by d.is_weekend
)
select case when is_weekend then 'Weekend' else 'Weekday' end as day_type,
   total_reviews,
   total_days,
   avg_reviews_per_day,
from daily_reviews
order by is_weekend desc