with 
school_years as (
    select *
    from {{ ref('int_school_years') }}
),

teachers as (
    select * 
    from {{ ref('dim_teachers')}}
),

sections as (
    select 
        teacher_id,
        school_year,
        course_name,
        case when teacher_id is not null then 1 else 0 end as is_active,
        min(section_started_at) as started_teaching_at
    from {{ ref('int_active_sections') }}
    where teacher_id is not null 
    {{ dbt_utils.group_by(4) }}
),

combined as (
    select 
        teachers.user_id as teacher_id,
        school_years.school_year,
        is_active,

        lag(is_active) over (
            partition by teachers.user_id 
            order by school_years.school_year
        ) as is_active_previous_year,
        
        max(is_active) over (
            partition by teachers.user_id 
        ) as is_active_all_years,
        
        listagg(sections.course_name, ', ') 
            within group (order by sections.course_name) as courses_taught
    
    from teachers 
    join school_years 
        on teachers.created_at
            between school_years.started_at 
            and school_years.ended_at
    left join sections 
        on teachers.user_id = sections.teacher_id
        and school_years.school_year = sections.school_year
    {{ dbt_utils.group_by(3) }}
),

final as (
    select 
        teacher_id,
        school_year,
        courses_taught,

        case when is_active 
             and not is_active_previous_year
             then 'active - new teacher'
            
            when is_active 
             and is_active_previous_year
             then 'active - returning teacher'

            when not is_active 
             and is_active_previous_year
             then 'inactive - former teacher'

            when not is_active 
             and not is_active_previous_year
             and is_active_all_years
             then 'inactive - churned'
            
            when not is_active_all_years
             then 'market'

            else (is_active || is_active_previous_year || is_active_all_years)
        end as teacher_status

    from combined 
)

select * 
from final 
