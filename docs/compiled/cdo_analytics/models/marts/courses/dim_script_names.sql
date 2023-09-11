with 
script_names as (
    select * 
    from "dev"."dbt_allison"."seed_script_names"
)

select * 
from script_names