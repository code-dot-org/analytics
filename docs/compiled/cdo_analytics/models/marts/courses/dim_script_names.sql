with 
script_names as (
    select * 
    from "dev"."dbt_jordan"."seed_script_names"
)

select * 
from script_names