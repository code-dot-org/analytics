{#

    -- FUTURE WORK: this should be built as an incremental model.  Some of the increment criteria
    -- are tricky since this is an aggregated metrics table where the current month will update every
    -- day, but past months should remain unchanged.  

    @bfranke here we are going to actually use dbt Cloud for the incrementation.
    this model will get a #MIR or #monthly tag in dbt Cloud and will be connected to a deployment run that runs (only) on the last day of the month. 
    
    It will be a part of a ritual called like "monthly reporting" or something where we take 15 min to run all the monthly reports and validate the numbers before we send them out. likely last day of the month or the day before. also, the underlying staging models are already running incrementally, so we are simply letting the model increment for a month and then see what happened.

#}

with
sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins') }}
),

users as (
    select
        user_id,
        user_type,

        case when is_international = 1 then 'International'
             when is_international = 0 then 'United States'
             else 'Missing' end as us_intl_label,
        -- us_intl,
        country
    from {{ ref('dim_users') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

final as (
    select
        u.user_type                         as "User Type",
        u.country                           as "Country",
        u.us_intl_label                     as "US/Intl",
        sy.school_year                      as "School Year",
        extract(year from si.sign_in_at)    as "Year",
        extract(month from si.sign_in_at)   as "Month",
        count(distinct si.user_id)          as "Total Distinct Users"

    from sign_ins as si
    left join users as u
        on si.user_id = u.user_id
    left join school_years as sy
        on sign_in_at
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(6) }}
)

select *
from final
