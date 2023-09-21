with 
course_names as (
    select * 
    from {{ ref('seed_course_names') }}
)

select * 
from course_names