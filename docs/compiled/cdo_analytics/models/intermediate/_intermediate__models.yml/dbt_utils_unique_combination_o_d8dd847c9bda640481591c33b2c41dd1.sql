





with validation_errors as (

    select
        user_id, level_id, script_id
    from "dev"."dbt_jordan"."int_user_levels"
    group by user_id, level_id, script_id
    having count(*) > 1

)

select *
from validation_errors


