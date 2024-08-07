with
pd_workshops as (
    select * 
    from {{ ref('base_dashboard_pii__pd_workshops') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    pd_workshop_id
    , organizer_id
    , sy.school_year
    , extract('year' from pdw.started_at)                               as cal_year
    , location_name
    , location_address
    , case 
        when course = 'CS Principles' then 'csp'
        when course = 'CS Discoveries' then 'csd'
        when course = 'Computer Science A' then 'csa'
        when course = 'CS Fundamentals' then 'csf'
        else 'other'
      end                                                               as course_name
    , subject
    , capacity
    , section_id
    , pdw.started_at
    , pdw.ended_at
    , created_at
    , updated_at
    , processed_at
    , regional_partner_id
    , is_on_map
    , is_funded
    , funding_type
    , module
from pd_workshops                                           as pdw
join school_years                                           as sy
    on pdw.started_at between sy.started_at and sy.ended_at