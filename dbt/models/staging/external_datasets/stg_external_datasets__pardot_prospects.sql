with 
source as (
    select *
    from {{ ref('seed_pardot_prospects') }}
),

prospects as (
    select 
        prospect_id,
        campaign,
        source, 
        
        hour_of_code_role                   as hoc_role,
        db_grades_taught                    as grades_taught,
        
        case when db_opt_in = 'yes'
             then 1 else 0 end              as is_opt_in,

        db_forms_submitted                  as last_submitted_forms,
        
        -- geographic information
        case when lower(db_country) in (
            'united states', 
            'us', 
            'usa', 
            'u.s.a',
            'united states of america')
            then 'us' else lower(db_country) 
            end                                 as country,
        db_state                                as state, 
        db_city                                 as city,
        
        -- dates
        created_date                            as registered_at,
        trunc(created_date)                     as registered_dt,
        date_trunc('year',created_date)         as cal_year,
        updated_date                            as updated_at,
        last_activity_at
    from source 
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
        pp.campaign,
        pp.source,
        pp.hoc_role,
        pp.is_opt_in,
        pp.country,
        sa.state_abbreviation as state,
        pp.city,
        pp.last_submitted_forms,
        
        sy.school_year,
        pp.cal_year,
        pp.registered_dt,
        pp.registered_at,
        pp.last_activity_at
    from prospects as pp
    
    left join state_abbr as sa 
        on lower(pp.state) = lower(sa.state_abbreviation)
        or lower(pp.state) = lower(sa.state_name)
    
    join school_years as sy 
        on pp.registered_at
        between sy.started_at
            and sy.ended_at )

select * 
from combined
