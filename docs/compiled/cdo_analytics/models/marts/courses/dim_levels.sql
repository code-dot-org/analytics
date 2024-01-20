with 
levels as (
    select * 
    from "dev"."dbt_jordan"."stg_dashboard__levels"
)

select * 
from levels