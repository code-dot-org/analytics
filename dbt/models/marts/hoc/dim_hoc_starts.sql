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

country_metadata as (
    select * 
    from {{ref('dim_country_reference')}}
),

final as (
    select 
        hoc_activity.hoc_start_id
        , hoc_activity.started_at
        , sy.school_year_int                                                as cal_year 
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
        , lower(hoc_activity.city) as city
        , hoc_activity.country
        , country_metadata.iso2 as country_code
        , lower(hoc_activity.state) as state
        , hoc_activity.state_code
    from hoc_activity 
    join school_years                                                       as sy 
        on hoc_activity.started_at 
            between sy.started_at 
            and sy.ended_at
    left join internal_tutorials                                            as it
        on hoc_activity.tutorial = it.tutorial_codes 
    left join country_metadata 
        on hoc_activity.country = country_metadata.country
)

select *
from final