with
pd_attendances as (
    select * 
    from {{ ref('base_dashboard_pii__pd_attendances') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    pda.*,
    sy.school_year
from pd_attendances pda
join school_years sy on pda.created_at between sy.started_at and sy.ended_at
where pda.created_at > {{ get_cutoff_date() }} 