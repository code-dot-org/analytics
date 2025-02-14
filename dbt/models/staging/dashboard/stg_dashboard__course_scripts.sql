with 
course_scripts as (
    select 
        id as course_script_id
        , course_id
        , script_id
        , position
    from {{ ref('base_dashboard__course_scripts')}}
)

select * 
from course_scripts