





with validation_errors as (

    select
        teacher_id, school_year, course_name, section_id
    from "dev"."dbt_jordan"."int_active_sections"
    group by teacher_id, school_year, course_name, section_id
    having count(*) > 1

)

select *
from validation_errors


