-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }}
),

renamed as (
    select 
        instructor_id, -- teacher_id 
        section_id,
        invited_by_id, -- teacher who invited
        status, 
        created_at,
        updated_at
    from section_instructors)

select * 
from renamed