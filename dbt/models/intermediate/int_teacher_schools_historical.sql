{# Notes:
Design: 1 row per teacher, school year
Logic: For every school year, assign the latest school the teacher was associated with in that school year. 
#}

with 
user_school_infos as (
    select * 
    from {{ ref('stg_dashboard_pii__user_school_infos') }}
),

school_infos as (
    select * 
    from {{ ref('stg_dashboard__school_infos') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

user_school_infos_sy as (
    select 
        usi.user_id,
        
        usi.school_info_id,
        si.school_id,
        
        sy.school_year as started_at_sy,
        usi.started_at,
        usi.ended_at,
        
        row_number() over (partition by usi.user_id, sy.school_year order by usi.started_at desc) as row_num

    from user_school_infos usi
    left join school_infos si 
        on usi.school_info_id = si.school_info_id
    join school_years sy 
        on usi.started_at 
            between sy.started_at 
                and sy.ended_at
),

final as (
    select 
        usi_sy.user_id as teacher_id,
        usi_sy.started_at_sy,
        usi_sy.started_at, 
        usi_sy.ended_at,
        usi_sy.school_info_id,
        usi_sy.school_id
    from user_school_infos_sy as usi_sy 
    where row_num = 1
    order by started_at_sy
)

select * 
from final 