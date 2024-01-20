





with validation_errors as (

    select
        school_year, teacher_id
    from "dev"."dbt_jordan"."dim_teacher_status"
    group by school_year, teacher_id
    having count(*) > 1

)

select *
from validation_errors


