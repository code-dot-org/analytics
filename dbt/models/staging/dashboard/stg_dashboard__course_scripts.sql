with 
course_scripts as (
    select * 
    from {{ ref('base_dashboard__course_scripts')}}
)

select * 
from course_scripts