{{
    config(
        materialized='incremental',
        schema='silver',
        unique_key='reviewer_id',
        incremental_strategy='merge',
        merge_update_columns=['reviewer_name', '_loaded_at']
    )
}}


with cte as (
select *
from {{ref("bronze_reviews")}}
),
 rw_name as (
select reviewer_id,
    reviewer_name,
    -- a try to find the last non null reviewer name    
    row_number() over (
        partition by reviewer_id
        order by case when reviewer_name is not null then 0 else 1 end,
        unix_review_time desc
    ) as rk
from cte
  
),
first_time_reveiw as (
    select reviewer_id,
    min(unix_review_time) as first_review_timestamp
from cte

group by reviewer_id
) 
select 
    r.reviewer_id,
    n.reviewer_name,
    r.first_review_timestamp,
    current_timestamp() as _loaded_at
from first_time_reveiw r
join rw_name n on r.reviewer_id = n.reviewer_id and n.rk = 1