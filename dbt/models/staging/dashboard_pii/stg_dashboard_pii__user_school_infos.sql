with 
user_school_info as (
    select * 
    from {{ ref('base_dashboard_pii__user_school_infos')}}
)

select * 
from user_school_info