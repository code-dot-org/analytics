-- model: stg_dashboard_section_instructors
-- scope: all teachers who created sections since 10/17/2023 when the co-teacher feature was added to the platform. 

with 
section_instructors as (
    select *
    from {{ ref('base_dashboard__section_instructors') }}
),

sections as (
    select distinct
        section_id, teacher_id
    from {{ ref('base_dashboard__sections') }}
),

combined as (
    select sei.*,
    
        case when sei.status = 0 then 'active'
             when sei.status = 1 then 'invited'
             when sei.status = 2 then 'declined'
             when sei.status = 3 then 'removed'
        else null end as section_instructor_status,

        case
            when sec.teacher_id is not null
                then 1
            else 0
        end as is_section_owner

    from section_instructors    as sei 
    left join sections          as sec 
        on sei.section_id = sec.section_id 
       and sei.instructor_id = sec.teacher_id 
),

final as (
    select 
        -- coteacher
        instructor_id       as teacher_id,
        section_id,
        invited_by_id       as invited_by_teacher_id,
        section_instructor_status,
        is_section_owner,

        -- dates
        created_at,
        updated_at,
        deleted_at
    from combined)

select *
from final