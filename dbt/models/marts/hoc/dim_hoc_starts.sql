with 

hoc_activity as (
    select * 
    from {{ ref('stg_pegasus_pii__hoc_activity') }}
    where hoc_start_id is not NULL
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

internal_tutorials as (
    select *
    from {{ ref('seed_hoc_internal_tutorials') }}
),

final as (
    select 
        hoc_activity.hoc_start_id
        , hoc_activity.started_at                                            as started_at
        , extract(year from hoc_activity.started_at)                                           as cal_year 
        , sy.school_year
        --, hoc_activity.referer
        , hoc_activity.company
        , hoc_activity.tutorial
        , case
            when it.is_internal is null then 1
            else 0
            end as is_third_party
        , case 
            when tutorial in ('tynkerapp','carnegie_accelerate')
            then 1
            else 0
            end as is_flagged_for_quality
        , hoc_activity.city                                  as city
        , hoc_activity.country                                as country
        , hoc_activity.state
        , hoc_activity.state_code
        --, hoc_activity.country_code
    from hoc_activity 
    join school_years                                                       as sy 
        on hoc_activity.started_at 
            between sy.started_at 
            and sy.ended_at
    left join internal_tutorials                                            as it
        on hoc_activity.tutorial = it.tutorial_codes )

select *
from final