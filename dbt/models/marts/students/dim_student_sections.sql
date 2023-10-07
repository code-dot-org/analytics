with 
followers as (
    select * 
    from {{ ref('stg_dashboard__followers')}}
),

sections as (
    select * 
    from {{ ref('stg_dashboard__sections')}}
),

school_years as (
    select * 
    from {{ ref('int_school_years')}}
),

users as (
    select *
    from {{ ref('stg_dashboard__users')}}
),

combined as (
    select 
        sy.school_year,
        followers.student_user_id,
        sections.user_id as teacher_user_id,
        sections.section_id, 
        sections.grade,
        users.age_years,
        -- datediff(year,users.birthday,sy.started_at) as age_at_start,
        rank() over(partition by followers.student_user_id, sy.school_year
                    order by followers.created_at, 
                        sections.first_activity_at,
                        sections.script_id,
                        sections.created_at
        ) as rnk
    from followers 
    left join sections 
        on followers.section_id = sections.section_id 
    left join school_years as sy
        on followers.created_at between sy.started_at and sy.ended_at
    left join users 
        on followers.student_user_id = users.user_id 
)

select * 
from combined 