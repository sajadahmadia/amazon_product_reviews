{{
    config(
        severity='warn'
    )
}}

-- helpful_votes should be <= total_votes
select 
    review_sk,
    helpful_votes,
    total_votes
from {{ ref('silver_fct_review') }}
where helpful_votes > total_votes
    and total_votes is not null
    and helpful_votes is not null