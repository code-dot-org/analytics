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
        followers.student_id,
        sections.user_id            as teacher_id,
        sections.section_id,
        row_number() over(
            partition by 
                followers.student_id, 
                sy.school_year
                order by 
                    followers.created_at
        ) as row_num
    from followers 
    left join sections 
        on followers.section_id = sections.section_id 
    left join school_years as sy
        on followers.created_at between sy.started_at and sy.ended_at
    left join users 
        on followers.student_id = users.user_id 
)

select
    student_id,
    section_id, 
    teacher_id,
    school_year
from combined 
where row_num = 1