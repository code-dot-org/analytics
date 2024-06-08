-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }}
),

sections as (
    select section_id, teacher_id 
    from {{ ref('stg_dashboard__sections')}}
    where section_id in (
        select section_id 
        from section_instructors)
),

final as (
    select 
        sei.instructor_id   as teacher_id,
        sei.section_id,

        case when sei.instructor_id = sec.teacher_id 
        then 1 else 0 end   as is_section_owner,

        sei.invited_by_id   as invted_by_teacher_id,
        sei.status,
        sei.created_at,
        sei.updated_at,
        sei.deleted_at
    from section_instructors    as sei 
    left join sections          as sec 
        on sei.section_id = sec.section_id )

select * 
from final