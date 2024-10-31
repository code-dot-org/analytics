with 
prospects as (
    select * 
    from {{ ref('stg_external_datasets__pardot_prospects') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select 
        sy.school_year,
        pp.country,
        pp.state,
        count(pp.prospect_id) as prospect_count
    from prospects as pp
    join school_years as sy 
        on pp.last_activity_at
        between sy.started_at
            and sy.ended_at
    {{ dbt_utils.group_by(3) }} )

select * 
from combined
order by 1,3 