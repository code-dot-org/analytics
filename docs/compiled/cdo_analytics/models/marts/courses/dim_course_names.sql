with 
course_names as (
    select * 
    from "dev"."dbt_jordan"."seed_course_names"
)

select * 
from course_names