{#
FUTURE WORK: this should be built as an incremental model.  Some of the increment criteria
are tricky since this is an aggregated metrics table where the current month will update every
day, but past months should remain unchanged.  
#}

with all_users as (
    select
        user_id,
        created_at,
        -- user_type,
        is_international,
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
        -- u.user_type,
        u.country,
        case when u.is_international = 1 then 'international' 
             when u.is_international = 0 then 'us' 
             else 'unknown' end as us_intl,
        sy.school_year                  as created_at_school_year,
        date_part(year, u.created_at)   as created_at_year,
        date_part(month, u.created_at)  as created_at_month,
        count(distinct u.user_id)       as num_accounts
    from all_users as u
    left join school_years as sy
        on u.created_at
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(5) }}
)

select *
from final
