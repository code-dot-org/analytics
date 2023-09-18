

with sections as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__sections"
    where created_at >= '2023-01-01'
)

select * 
from sections

