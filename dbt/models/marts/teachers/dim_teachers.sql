with 
teachers as (
    select 
        {{ dbt_utils.star(from=ref('dim_users'), 
            except=[
                "user_id", 
                "user_type", 
                "student_id"]) }}
    from {{ ref('dim_users') }}
    where user_type = 'teacher'
),

sections as (
    select * 
    from {{ ref('dim_sections') }}
    where teacher_id in (select teacher_id from teachers)
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

section_instructors as (
    select teacher_id, section_id
    from {{ ref('stg_dashboard__section_instructors') }}
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

combined as (
    select 
        teachers.teacher_id,
        sections.section_id,
        case 
            when section_instructors.teacher_id is not null 
                then 1 else 0 
            end as is_section_owner
    from teachers
    left join sections 
        on teachers.teacher_id = sections.teacher_id 
    left join section_instructors
        on sections.section_id = section_instructors.section_id
),

-- get teacher NCES school_id association
teacher_schools as (
    select 
        teachers.teachers_id,
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

final as (
    select 
        teachers.*, 
        comb.section_id,
        coalesce(comb.is_section_owner,0) as is_section_owner,
        ts.school_id,
        school_years.school_year as created_at_school_year
    from combined as comb 
    inner join teachers 
        on comb.teacher_id = teachers.teachers_id
    inner join teacher_schools as ts 
        on teachers.user_id = ts.user_id
        and ts.rnk = 1
    inner join school_years 
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at 
),

renamed as (
    select 
        -- user
        teacher_id,
        studio_person_id,
        school_id,
        school_info_id,
        is_urg,
        gender,
        locale,
        birthday,
        sign_in_count,
        total_lines,

        section_id,     
        is_section_owner,

        teacher_email,
        races,
        race_group,
        gender_group,

        is_international,
        us_intl,
        country,

        -- dates
        created_at_school_year,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at
    from final )

select *
from renamed