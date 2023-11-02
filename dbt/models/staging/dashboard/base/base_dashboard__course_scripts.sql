with 
source as (
    select * 
    from {{ source('dashboard', 'course_scripts') }}
),

renamed as (
    select
        id as course_script_id,
        course_id,
        script_id,
        position,
        experiment_name,
        default_script_id
    from source
)

select * 
from renamed  