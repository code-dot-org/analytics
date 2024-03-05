with
source as (
    select *
    from {{ source('dashboard','lti_sections') }}
),

renamed as (
    select 
        id as lti_section_id,
        lti_course_id,
        section_id,
        lms_section_id,
        created_at,
        updated_at,
        deleted_at
    from source)

select * 
from renamed