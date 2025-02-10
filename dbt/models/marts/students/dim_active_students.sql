-- model: dim_active_students 

with 
users as (
    select 
        *, 

        case 
            when gender_group = 'f'     then 1 
            when gender_group in (
                    'not_collected', 
                    'no_response')      then null 
            when gender_group is null   then null 
        else 0 end as is_female

    from {{ ref('dim_users') }}
    where user_type = 'student'
),

section_mapping as (
    select * 
    from {{ ref('int_section_mapping') }}
),

user_levels as (
    select  

        user_id,
        created_date    as activity_date,
        1               as has_user_level_activity,
        null            as has_project_activity
        
    from {{ ref('dim_user_levels') }}
    where 
        total_attempts > 0 
        and created_date > {{ get_cutoff_date() }} 
), 

projects as (
    select 

        user_id,
        project_created_at::date    as activity_date,
        null                        as has_user_level_activity,
        1                           as has_project_activity

    from {{ ref('dim_student_projects') }}
    where 
        user_type = 'student'
        and project_created_at > {{ get_cutoff_date() }} 
),

unioned as (
    select * from user_levels
    union all 
    select * from projects 
), 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        uni.user_id                             as student_id,
        
        usr.us_intl,
        usr.country,

        usr.is_female,
        usr.is_urg,

        uni.activity_date,
        extract(year from uni.activity_date)   as cal_year,
        sy.school_year,

        sm.section_id,
        sm.teacher_id,
        sm.school_id,
        sm.school_district_id,

        max(uni.has_user_level_activity)       as has_user_level_activity,
        max(uni.has_project_activity)          as has_project_activity

    from unioned    as uni
    join users      as usr
        on uni.user_id = usr.user_id
    
    join school_years as sy 
        on uni.activity_date 
            between sy.started_at
                and sy.ended_at 
    
    left join section_mapping sm 
        on uni.user_id = sm.student_id
            and sy.school_year = sm.school_year

    {{ dbt_utils.group_by(12) }} 
),

final as (
    select 
        *, 
        1 as is_active_student

    from combined 
    where coalesce(
            has_project_activity,
            has_user_level_activity) = 1 )

select * 
from final
