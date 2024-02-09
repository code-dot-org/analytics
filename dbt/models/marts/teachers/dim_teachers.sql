with 
teachers as (
    select 
        {{ dbt_utils.star(from=teachers),
            except=['teacher_id','user_type'] }}
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
        teachers.user_id as teacher_id,
        si.school_id,
        rank () over (
            partition by teachers.teacher_id 
            order by si.school_id, usi.ended_at desc) rnk
    from teachers
    left join user_school_infos as usi    
        on usi.user_id = teachers.teacher_id
    left join school_infos as si 
        on si.school_info_id = usi.school_info_id
),

-- teacher_latest_school as (
--     select 
--         teacher_id,
--         school_id
--     from teacher_schools
--     where rnk = 1
-- ),

final as (
    select 
        -- all user data from dim_users
        teachers.*, 

        -- foreign keys
        ts.school_id,
        school_years.school_year as created_at_school_year
    from teachers 
    left join teacher_schools as ts 
        on teachers.teacher_id = ts.teacher_id
    left join school_years 
        on teachers.created_at 
            between school_years.started_at 
                and school_years.ended_at)

select * 
from final
