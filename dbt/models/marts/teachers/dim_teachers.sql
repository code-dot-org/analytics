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
        and created_at >= '2023-01-01'
),

user_geos as (
    select 
        user_id,
        is_international,
        lower(country) as country
    from {{ ref('stg_dashboard__user_geos')}}
    where user_id in (select user_id from teachers)
),

    -- next work for this model...
 {# 
    school_info as (
        select *
        from {{ ref('dim_schools) }}
    ),
  #}

combined as (
    select 
        -- teacher info
        teachers.user_id as teacher_user_id,
        teachers.gender,
        teachers.is_urm,
        teachers.races,
        teachers.is_active,
        teachers.school_info_id,
        
        {# -- school info 
        school_info.school_id,
        school_info.school_name,
        school_info.started,
        school_info.started_at,
        school_info.trained,
        school_info.trained_this_year, #}


        -- user_geo info
        ug.is_international,
        ug.country
    from teachers 
    {# left join on school_info 
        on teachers.school_info_id = school_info.school_info_id #}
    left join user_geos as ug 
        on teachers.user_id = ug.user_id
)

select * 
from combined