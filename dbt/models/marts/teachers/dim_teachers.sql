with 
teachers as (
    select {{ dbt_utils.star(
        from=ref('stg_dashboard__users'),
        except=[
            "admin",
            "birthday",
            "primary_contact_info_id"]) }}
    from {{ ref('stg_dashboard__users')}}
    where user_type = 'teacher'
        and purged_at is null 
),

user_geos as (
    select user_id 
    from {{ ref('stg_dashboard__user_geos') }}
    where user_id in (select user_id from teachers)
        and is_international = 0 
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

teacher_years as (
    select 
        user_id,
        school_year 
    from teachers 
    join school_years as sy 
        on teachers.created_at < sy.ended_at
        and sy.started_at < current_date  
),

{# course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
), #}

user_school_info as (
    select 
        user_id,
        school_info_id,
        started_at,
        ended_at
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_info as (
    select *
    from {{ ref('dim_schools') }}
),

combined as (
    select distinct 
        teachers.user_id as teacher_user_id,
        school_info.school_year,
        teachers.school_info_id,
        school_info.school_id,
        school_info.school_name

        -- (js) I don't understand how to bring in the course_name,
        -- in the proc (run_rosetta_v2) a 1=1 join is used but idk why...
        {# course_structure.course_name #}

        -- missing values to be sourced
        -- from csf data models... @allison-code-dot-org
        {# 
        started,
        started_at,
        trained,
        trained_this_year
        #}

    from teachers 
    join user_geos 
        on teachers.user_id = user_geos.user_id 
    left join teacher_years 
        on teachers.user_id = teacher_years.user_id
    left join user_school_info 
        on teachers.user_id = user_school_info.user_id 
    left join school_info 
        on user_school_info.school_info_id = school_info.school_info_id 
    
    {# left join course_structure 
        on 1=1 #}
)

select * 
from combined