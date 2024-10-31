with 
prospects as (
    select 
        prospect_id,
        last_activity_at,
        case when lower(pp.country) in (
            'united states', 
            'us', 
            'usa', 
            'united states of america')
            then 'us' else lower(country) 
            end as country,
        state 
    from {{ ref('stg_external_datasets__pardot_prospects') }}
),

state_abbr as (
    select * 
    from {{ ref('seed_state_abbreviations') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        sy.school_year,
        pp.country,
        sa.state,
        count(pp.prospect_id) as prospect_count
    from prospects as pp
    
    left join state_abbr as sa 
        on lower(pp.state) = lower(sa.state_abbreviation)
        or lower(pp.state) = lower(sa.state_name)
    
    join school_years as sy 
        on pp.last_activity_at
        between sy.started_at
            and sy.ended_at
    {{ dbt_utils.group_by(3) }} )

select * 
from combined
order by 1,3 