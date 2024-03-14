-- model: stg_dashboard_section_instructors
-- scope: all teachers who created sections since 10/17/2023 when the co-teacher feature was added to the platform. 

with 
section_instructors as (
    select *
    from {{ ref('base_dashboard__section_instructors') }}
),

final as (
    select 
        -- coteacher
        instructor_id       as teacher_id,
        section_id,
        invited_by_id       as invited_by_teacher_id,
        status,

        -- dates
        created_at,
        updated_at,
        deleted_at

    from section_instructors)

select *
from final