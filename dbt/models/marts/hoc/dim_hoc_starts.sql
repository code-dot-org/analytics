with 

hoc_activity as (
    select * 
    from {{ ref('stg_pegasus_pii__hoc_activity') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    hoc_activity.hoc_start_id
    , hoc_activity.started_at
    , sy.school_year_int                                                as cal_year 
    , sy.school_year
    -- , hoc_activity.referer
    , hoc_activity.company
    , hoc_activity.tutorial
    , hoc_activity.is_third_party
    , hoc_activity.city
    , hoc_activity.country
    , hoc_activity.state
    , hoc_activity.state_code
    , hoc_activity.country_code
from hoc_activity 
join school_years                                                       as sy 
    on hoc_activity.started_at between sy.started_at and sy.ended_at

