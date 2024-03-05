-- model: dim_section_instructors
-- scope: all teachers who created sections since 10/17/2023 when the co-teacher feature was added to the platform. 

-- option 1: dim_section_instructors
-- this is BP in terms of bringing in this data. we would then affect dim_teachers using this dim model.


with 
section_instructors as (
    select *, 
        case when invited_by_id is null -- wasn't invited 
            then 1 else 0 
        end as is_section_owner,

    from {{ ref('stg_dashboard__section_instructors') }}
),

sections as (
    select *
    from {{ ref('dim_sections') }}
),

combined as (
    select 
        -- coteacher
        section_instructors.instructor_id   as coteacher_id,
        section_instructors.status          as coteacher_status,
        section_instructors.invited_by_id   as invited_by_teacher_id,
        section_instructors.section_id,
        section_instructors.created_at      as coteacher_created_at,
        section_instructors.updated_at      as coteacher_updated_at,

        -- related section information
        sections.section_name,
        sections.script_id,
        sections.course_name,
        sections.is_active,
        sections.section_started_at
    from section_instructors
    left join sections
        on section_instructors.section_id = sections.section_id
)

select *
from combined

