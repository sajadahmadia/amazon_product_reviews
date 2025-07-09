{{
    config(
        severity='error'
    )
}}

-- we shouldn't have reviews in the future
select 
    review_sk,
    review_date,
    current_date() as today
from {{ ref('silver_fct_review') }}
where review_date > current_date()