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
    , lower(location_name)                                              as location_name
    , lower(location_address)                                           as location_address
    , case 
        when course = 'CS Principles' then 'csp'
        when course = 'CS Discoveries' then 'csd'
        when course = 'Computer Science A' then 'csa'
        when course = 'CS Fundamentals' then 'csf'
        else lower(course)
      end                                                               as course_name
    , case 
        when course = 'CS Principles' then '9_12'
        when course = 'CS Discoveries' then '6_8'
        when course = 'Computer Science A' then '9_12'
        when course = 'CS Fundamentals' then 'k_5'
        else null
      end                                                               as grade_band
    , case 
        when lower(course) = 'build your own workshop' then 1 else 0 
    end                                                                 as is_byow
    , lower(subject)                                                    as subject
    , capacity
    , section_id
    , pdw.started_at
    , pdw.ended_at
    -- , case 
    --     when datediff(day, pdw.started_at, pdw.ended_at) > 0 
    --     then datediff(day, pdw.started_at, pdw.ended_at)
    --     else null 
    -- end                                                                 as num_days
    -- , case 
    --     when datediff(day, pdw.started_at, pdw.ended_at) = 0
    --     then datediff(hour, pdw.started_at, pdw.ended_at)
    --     else datediff(day, pdw.started_at, pdw.ended_at) * 8
    -- end                                                                 as num_hours
    , created_at
    , updated_at
    , processed_at
    , regional_partner_id
    -- , is_on_map
    -- , is_funded
    -- , funding_type
    , module
from pd_workshops                                           as pdw
join school_years                                           as sy
    on pdw.started_at between sy.started_at and sy.ended_at