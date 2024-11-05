with 
prospects as (
    select 
        prospect_id,
        created_at,
        date_trunc('year',created_at) as created_year,
        date_trunc('week',created_at) as created_week,
        date_trunc('month',created_at) as created_month,
        last_activity_at,
        last_submitted_at,

        case when lower(country) in (
            'united states', 
            'us', 
            'usa', 
            'u.s.a',
            'united states of america')
            then 'us' else lower(country) 
            end as country,
        state,
        city
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
        pp.last_submitted_at,
        pp.created_at,
        pp.created_week,
        pp.created_month,
        pp.created_year,
        sy.school_year,
        pp.country,
        sa.state_abbreviation as state,
        pp.city
    from prospects as pp
    
    left join state_abbr as sa 
        on lower(pp.state) = lower(sa.state_abbreviation)
        or lower(pp.state) = lower(sa.state_name)
    
    join school_years as sy 
        on pp.created_at
        between sy.started_at
            and sy.ended_at )

select * 
from combined