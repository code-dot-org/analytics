





with validation_errors as (

    select
        user_id, course_id, script_id, stage_id
    from "dev"."dbt_jordan"."dim_user_stages"
    group by user_id, course_id, script_id, stage_id
    having count(*) > 1

)

select *
from validation_errors


