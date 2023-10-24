-- Model: dim_school_course_status
-- Scope: course status data collected for active courses only collected by school
-- Note: follow school_course_status; remove lead/lag() component
-- Statuses:
    -- active retained: had an active section last SY and has an active section this SY (could be the same section ID, but needs to have 5+ active students both SYs)
    -- active reacquired: did not have an active section last SY, but does have one this SY
    -- active new: never has had an active section before, but has one this SY
    -- inactive churn: did not have an active section last SY and does not have an active section this SY
    -- inactive this year: had an active section last SY, does not have one this SY
    -- market: has a teacher account but has never had an active section
-- Author: js
with 
teachers as (
    select 
        school_id,
        school_year,
        teacher_id
    from {{ ref('dim_teachers') }}
    where is_international = 0 
),

sections as (
    select * 
    from {{ ref('dim_sections') }}
    where is_active
),

students as (
    select * 
    from {{ ref('dim_students') }}
    where is_international = 0 
),

-- end product:
final as (
    select 
        school_id,
        school_year,
        course_name,

        -- course_status: this logic needs heavy review @bakerfranke @allison-code-dot-org 
        case when seq = 1 then 'active new' 
             when seq > 1 then 'active retained'
             when seq is null 
              and lag(seq,1) over(
                    partition by school_id, course_name 
                    order by school_year) is not null 
                then 'inactive this year'

            when seq is null
             and sum(seq) over (
                    partition by school_id, course_name 
                    order by school_year rows unbounded preceding) is not null 
             and lag(seq) over (
                    partition by school_id, course_name 
                    order by school_year) is not null 
                then 'inactive churn'

            when seq > 1 
             and lag(seq,1) over(
                    partition by school_id, course_name 
                    order by school_year) is null 
                then 'active reacquired'

            else 'market'
        end as course_status, 
        
        -- (js) flag based on presence of course activity
        case when seq is not null then 1 else 0 end as is_active,

        -- (js) aggregating here to ensure our grain
        sum(num_students_started) as total_students_started,
        sum(num_teachers_started) as total_teachers_started,
        max(seq) as total_years_active
    from combined
    
    {{ dbt_utils.group_by(5) }}
)

select * 
from final 