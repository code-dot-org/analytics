-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }} 
    -- where invited_by_id is not null
),

final as (
    select 
    instructor_id as teacher_id,
    section_id,
    invited_by_id,
    status,
    created_at,
    updated_at,
    deleted_at
)

select * 
from section_instructors