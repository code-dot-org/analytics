with 
course_names as (
    select * 
    from "dev"."dbt_allison"."seed_course_names"
)

select * 
from course_names