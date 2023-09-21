with 
levels as (
    select * 
    from "dev"."dbt_allison"."stg_dashboard__levels"
)

select * 
from levels