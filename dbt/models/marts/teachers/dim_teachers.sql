
-- model: dim_teachers__v2
-- scope: all teachers, no including co-teachers 
-- ref: dataops-548

with 
teachers as (
    select {{ dbt_utils.star(ref('dim_users'),
        except=[
            "user_id",
            "user_type",
            "student_id"])}}
    from {{ ref('dim_users') }}                
    where user_type = 'teacher'
),

coteachers as (
    select distinct 
        teacher_id,
        section_id,
        is_section_owner
    from {{ ref('dim_section_instructors') }}
),

school_years as (
    select * 
    from {{ref('int_school_years') }}
),

user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
    where user_id in (
        select teacher_id 
        from teachers)
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
    where school_info_id in (
        select school_info_id 
        from user_school_infos)
), 

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.teacher_id,
        si.school_id,
        rank () over (
            partition by teachers.teacher_id 
            order by usi.ended_at desc) as rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.teacher_id
    left join school_infos as si 
        on si.school_info_id = usi.school_info_id
),

combined as (
    select 
        --teacher info
        teachers.*,
        coteachers.is_section_owner,

        --school info
        school_years.school_year,
        teacher_schools.school_id
        
    from teachers 
    inner join teacher_schools
        on teachers.teacher_id = teacher_schools.teacher_id
        and teacher_schools.rnk = 1
    inner join school_years
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at
    left join coteachers 
        on teachers.teacher_id = coteachers.teacher_id    
        and teachers.section_id = coteachers.section_id
),

final as (
    select 
        teacher_id,
        school_year,
        school_id,
        school_info_id,
        teacher_email, -- PII!
        is_section_owner,
        races,
        race_group,
        gender,
        gender_group,
        birthday,
        age_years,
        locale,
        country,
        is_international,
        us_intl,
        total_lines,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        purged_at,
        deleted_at
    from combined)

select *
from final 