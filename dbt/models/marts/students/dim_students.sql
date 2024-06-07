with 
students as (
    select *    
    from {{ ref('dim_users')}}
    where user_type = 'student'
),

school_years as (
    select * from {{ref('int_school_years')}}
),

school_infos as (
    select *
    from {{ ref("stg_dashboard__school_infos")}}
),

final as (
    select 
        students.*, 
        sch.school_id,
        sy.school_year as created_at_school_year
    from students 
    left join school_years as sy 
        on students.created_at 
            between sy.started_at 
                and sy.ended_at
    left join school_infos as sch
        on students.school_info_id = sch.school_info_id )

select * 
from final 