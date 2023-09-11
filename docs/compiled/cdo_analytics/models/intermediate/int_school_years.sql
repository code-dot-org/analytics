with 
school_years as (
    select * 
    from "dev"."dbt_allison"."seed_school_years"
)

select * 
from school_years