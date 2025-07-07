{{
    config(
        materialized='table',
        schema='gold'
    )
}}
-- Answer to the main question:
-- here, I assumed that the main question asked to find the avg rating per year-month across the top five product categories

-- finding the top 5 product categories, based on the number of times that their products were reviewed
with top_five_categories as (
select p.primary_category, 
    count(distinct p.product_id) as product_count
from {{ ref('silver_fct_review') }} fct
inner join {{ ref('silver_dim_products')}} p on p.product_id = fct.product_id
where p.primary_category is not null
group by p.primary_category
order by product_count desc
limit 5
)
select dt.year_month,
    p.primary_category,
    avg(fct.rating) as avg_rating
from {{ ref('silver_fct_review') }} fct
inner join {{ ref('silver_dim_products')}} p on p.product_id = fct.product_id 
inner join {{ref('silver_dim_date')}} dt on dt.date_id = fct.date_id
where p.primary_category in (select primary_category 
                                from top_five_categories)
group by dt.year_month, p.primary_category