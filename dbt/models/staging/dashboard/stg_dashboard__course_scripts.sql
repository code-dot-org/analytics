with 
course_scripts as (
    select 
        course_script_id
        , course_id
        , script_id
        , position
        , lower(experiment_name)            as experiment_name
        , default_script_id 
    from {{ ref('base_dashboard__course_scripts')}}
)

select * 
from course_scripts