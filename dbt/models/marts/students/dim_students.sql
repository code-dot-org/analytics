with 
students as (
    select {{ dbt_utils.star(
            from=ref('stg_dashboard__users'),
            except=["admin","birthday","primary_contact_info_id"]) }}
    from {{ ref('stg_dashboard__users')}}
    where user_type = 'student'
        and purged_at is null 
        and created_at >= '2023-01-01'
),

user_geos as (
    select 
        user_id,
        is_international,
        lower(country) as country
    from {{ ref('stg_dashboard__user_geos')}}
    where user_id in (select user_id from students)
),

user_levels as (
    select 
        user_id                                         as student_user_id, 
        -- generate a list of levels/scripts in asc order of access 
        listagg(level_id,  ', ') 
            within group (order by user_level_id asc)   as list_levels,
        listagg(script_id,  ', ') 
            within group (order by user_level_id asc)   as list_scripts,
        max(user_level_id)                              as last_user_level_id,
        max(level_id)                                   as last_level_id,
        max(script_id)                                  as last_script_id,
        max(coalesce(updated_at,created_at))            as last_updated_at,
        sum(attempts)                                   as total_attempts,
        sum(is_submitted)                               as total_submissions,
        sum(time_spent)                                 as total_time_spent
    from {{ ref('stg_dashboard__user_levels') }}
    where user_id in (select user_id from students)
        and deleted_at is null 
    group by 1,2,3
    {# {{ dbt_utils.group_by('3') }} #}
),

combined as (
    select 
        
        /*  grain of this table is a *student*
            so how we include user_levels is tbd...
            in the meantime I made some suggestions */

        -- student info 
        ul.student_user_id,
        stu.gender,
        stu.is_urm,
        stu.races,
        stu.is_active,
        stu.school_info_id,
        
        -- user_geos 
        ug.is_international,
        ug.country,

        -- most recent user_level activity
        ul.last_user_level_id,
        ul.last_level_id,
        ul.last_script_id,
        ul.last_updated_at
        
    from user_levels                                as ul 
    left join students                              as stu
        on ul.student_user_id = stu.user_id 
    left join user_geos                             as ug
        on stu.user_id = ug.user_id
)

-- runtime: , rows retrieved: 
select * 
from combined 