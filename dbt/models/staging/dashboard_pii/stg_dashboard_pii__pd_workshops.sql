/*Edit log
- CK, May 2025 - added workshop name
*/

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
    , name                                                              as workshop_name
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
    --, module
    , lower(participant_group_type) as participant_group_type
    , case 
        when json_extract_path_text(properties, 'virtual') = 'true' then 1 
        when json_extract_path_text(properties, 'virtual') = 'false' then 0 
        else null 
    end as is_virtual
    -- , case 
    --     when pdw.started_at is null 
    --         or pdw.started_at > current_date() 
    --     then 1
    --     else 0                 
    -- end as is_upcoming
from pd_workshops                                           as pdw
join school_years                                           as sy
    on coalesce(pdw.started_at, pdw.created_at) between sy.started_at and sy.ended_at
where coalesce(pdw.started_at, pdw.created_at) > {{ get_cutoff_date() }} 