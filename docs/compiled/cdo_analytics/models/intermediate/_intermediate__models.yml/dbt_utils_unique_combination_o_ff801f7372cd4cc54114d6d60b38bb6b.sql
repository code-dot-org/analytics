





with validation_errors as (

    select
        student_id, school_year, section_id, teacher_id, school_id
    from "dev"."dbt_jordan"."int_section_mapping"
    group by student_id, school_year, section_id, teacher_id, school_id
    having count(*) > 1

)

select *
from validation_errors


