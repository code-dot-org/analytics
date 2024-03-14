-- model: stg_dashboard_section_instructors
-- scope: all teachers who created sections since 10/17/2023 when the co-teacher feature was added to the platform. 

with 
section_instructors as (
    select *
    from {{ ref('stg_dashboard__section_instructors') }}
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections') }}
    where created_at >= '2023-10-17'
),

combined as (
    select 
        si.*,
        case when sec.teacher_id is not null 
             then 1 else 0 
        end as is_section_owner
    from section_instructors    as si
    left join sections          as sec
        on si.section_id = sec.section_id
        and si.instructor_id = sec.teacher_id
),

final as (
    select 
        teacher_id,
        section_id,
        is_section_owner,
        invited_by_teacher_id,
        status,
        created_at,
        updated_at,
        deleted_at
    from section_instructors)

select *
from final