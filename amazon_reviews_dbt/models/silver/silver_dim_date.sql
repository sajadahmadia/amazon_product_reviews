{{
    config(
        materialized='table',
        schema='silver'
    )
}}


with date_bounds as (
select 
    date(timestamp_seconds(min(unix_review_time))) as start_date,
    date(timestamp_seconds(max(unix_review_time))) as end_date
from {{ref("bronze_reviews")}}
where unix_review_time is not null
),
date_spine as (
select date_day
from date_bounds,
unnest(generate_date_array(start_date, end_date, interval 1 day)) as date_day
)
select cast(format_date('%Y%m%d', date_day) as int64) date_id,
    date_day,
    extract(year from date_day) year,
    extract(month from date_day) month,
    extract(day from date_day) day,
    date_trunc(date_day, month) as year_month, 
    extract(dayofweek from date_day) as day_of_week,
    case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
    current_timestamp() as _loaded_at
from date_spine