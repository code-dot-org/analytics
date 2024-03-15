-- staging co-teacher data
-- scope: invited co-teachers
with
section_instructors as (
    select * 
    from {{ ref('base_dashboard__section_instructors') }} 
    -- where invited_by_id is not null
),

sections as (
    select distinct 
        teacher_id,
        section_id 
    from {{ ref('stg_dashboard__sections') }}
),

combined as (
    select 
        section_instructors.*,
        case when 
    from section_instructors 
    left join sections 
        on section_instructors.instructor_id = sections.user_id 
       and section_instructors.section_id = sections.section_id 
),

final as 

select * 
from section_instructors