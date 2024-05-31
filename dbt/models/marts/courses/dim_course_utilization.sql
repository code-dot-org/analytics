
with 
user_levels as (
    select * 
    from {{ ref('stg_dashboard__user_levels') }}
),

section_mapping as (
    select * 
    from {{ ref('int_section_mapping') }}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure')}}
),

users as (
    select *
    from {{ ref('dim_users') }}
),

combined as (
    select  
        -- course info
        ul.level_id,
        ul.script_id,
        cs.section_id,
        cs.course_name,

        -- dates 
        usr.school_year,
        ul.created_at as activity_at,

        -- utilization values 
        sum(ul.attepts)     as num_attempts,
        sum(ul.time_spent)  as total_time_spent,
        max(best_score)     as best_score

    from user_levels as ul 
    join users as usr 
        on ul.user_id = usr.user_id
    
    join course_structure as cs 
        on ul.level_id = cs.level_id
        and ul.script_id = cs.script_id
    
    {# join school_years as sy 
        on ul.created_at 
            between sy.start_date 
                and sy.end_date #}

    left join section_mapping as sm 
        on ul.user_id = sm.student_id
        
    {{ dbt_utils.group_by(12) }} )

select * 
from combined 
where activity_at >= '2024-01-01'

