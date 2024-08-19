with 

pd_intl_opt_ins as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_international_opt_ins') }}
), 

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
)

select 
    oi.* 
    , t.created_at                                              as account_created_at                                              
from pd_intl_opt_ins                                            as oi 
left join teachers                                              as t 
    on oi.teacher_id = t.teacher_id