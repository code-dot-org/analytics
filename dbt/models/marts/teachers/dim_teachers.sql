with 
teachers as (
    select *  
    from {{ ref('dim_users')}}
    where user_type = 'teacher'
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
    where school_id is not null
), 

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.user_id,
        si.school_id,
        rank () over (
            partition by teachers.user_id 
            order by usi.ended_at desc)                             as rnk
    from teachers
    left join user_school_infos                                     as usi    
        on usi.user_id = teachers.user_id
    left join school_infos                                          as si 
        on si.school_info_id = usi.school_info_id
),

--excludes student_id, cap_status, and cap_status_date from dim_users
final as (
    select 
        teachers.user_id,
        teachers.teacher_id,
        ts.school_id,
        teachers.user_type,
        teachers.studio_person_id,
        teachers.is_urg,
        teachers.gender,
        teachers.locale,
        teachers.birthday,
        teachers.sign_in_count,
        teachers.total_lines,
        teachers.current_sign_in_at,
        teachers.last_sign_in_at,
        teachers.created_at,
        teachers.updated_at,
        teachers.deleted_at,
        teachers.purged_at,
        teachers.teacher_email,
        teachers.races,
        teachers.race_group,
        teachers.gender_group,
        teachers.self_reported_state,
        teachers.country,
        teachers.us_intl,
        teachers.is_international,
        school_years.school_year                                    as created_at_school_year
    from teachers 
    inner join teacher_schools                                      as ts 
        on teachers.user_id = ts.user_id
        and ts.rnk = 1
    inner join school_years 
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at)

select * 
from final