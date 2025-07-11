{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with monthly_aggregates as (
select d.year,
    d.month,
    count(f.review_sk) as monthly_review_count,
    avg(f.helpful_votes) as monthly_avg_helpful_votes
from {{ ref('silver_fct_review') }} f
inner join {{ ref('silver_dim_date') }} d on d.date_id = f.date_id
group by d.year, d.month
)
select month,
    format_date('%B', date(2024, month, 1)) as month_name,
    sum(monthly_review_count) as total_reviews,
    round(avg(monthly_review_count), 2) as avg_reviews_per_month,
    round(avg(monthly_avg_helpful_votes), 2) as avg_helpful_votes
from monthly_aggregates
group by month
order by month  