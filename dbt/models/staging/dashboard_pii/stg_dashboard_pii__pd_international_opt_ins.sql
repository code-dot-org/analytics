with 
international_opt_ins as (
    select * 
    from {{ ref('base_dashboard_pii__pd_international_opt_ins')}}
)

select * 
from international_opt_ins 