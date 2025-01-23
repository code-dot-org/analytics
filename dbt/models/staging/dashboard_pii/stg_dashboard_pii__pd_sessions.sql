with
pd_sessions as (
    select * 
    from {{ ref('base_dashboard_pii__pd_sessions') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    pds.pd_session_id
    , pds.pd_workshop_id
    , sy.school_year
    , extract('year' from pds.started_at)                                                       as cal_year
    , pds.started_at
    , pds.ended_at
    , datediff(hour, pds.started_at, pds.ended_at)                                               as num_hours
    , pds.created_at
    , pds.updated_at
from pd_sessions                                                                                as pds
join school_years                                                                               as sy
    on pds.started_at between sy.started_at and sy.ended_at