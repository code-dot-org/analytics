with 

user_school_infos as (
    select 
        user_school_info_id,
        user_id,
        started_at,
        case when ended_at is not null then ended_at else '9999-12-31' end as ended_at,
        school_info_id,
        last_confirmation_at,
        created_at,
        updated_at 
    from {{ ref('base_dashboard_pii__user_school_infos') }}
)

select * 
from user_school_infos