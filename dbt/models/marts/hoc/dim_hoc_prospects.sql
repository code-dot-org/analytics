with 
prospects as (
    select 
        prospect_id,
        created_date,
        date_trunc('week',created_date) as created_week,
        date_trunc('month',created_date) as created_month,
        last_activity_at,

        case when lower(country) in (
            'united states', 
            'us', 
            'usa', 
            'u.s.a',
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
        pp.prospect_id,
        pp.last_activity_at,
        pp.created_date,
        pp.created_week,
        pp.created_month,
        sy.school_year,
        pp.country,
        sa.state_abbreviation as state
    from prospects as pp
    
    left join state_abbr as sa 
        on lower(pp.state) = lower(sa.state_abbreviation)
        or lower(pp.state) = lower(sa.state_name)
    
    join school_years as sy 
        on pp.last_activity_at
        between sy.started_at
            and sy.ended_at )

select * 
from combined