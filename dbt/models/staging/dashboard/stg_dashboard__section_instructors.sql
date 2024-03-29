-- model: stg_dashboard_section_instructors
-- scope: all teachers who created sections 
-- since 10/17/2023 when the co-teacher feature was added to the platform. 

with 
section_instructors as (
    select *
    from {{ ref('base_dashboard__section_instructors') }}
    where created_at >= '2023-10-17'
        and deleted_at is null 
),

combined as (
    select *,

        case when status = 0 then 'active'
             when status = 1 then 'invited'
             when status = 2 then 'declined'
             when status = 3 then 'removed'
        else null end as section_instructor_status

    from section_instructors
),

final as (
    select 
        
        -- coteacher
        instructor_id as teacher_id,
        section_id,
        invited_by_id as invited_by_teacher_id,
        section_instructor_status,
        
        -- dates
        created_at,
        updated_at
        {# deleted_at #}

    from combined )

select *
from final