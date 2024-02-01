{#
FUTURE WORK: this should be built as an incremental model.  Some of the increment criteria
are tricky since this is an aggregated metrics table where the current month will update every
day, but past months should remain unchanged.  
#}

with sign_ins as (
    select *
    from {{ ref('stg_dashboard__sign_ins')}}
),

users as (
    select
        user_id,
        user_type,
        is_international
    from {{ ref('dim_users') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

final as (
    select
        u.user_type,
        case when u.is_international = 1 then 'intl' else 'us' end as us_intl,
        sy.school_year as sign_in_school_year,
        extract(year from si.sign_in_at) as sign_in_year,
        extract(month from si.sign_in_at) as sign_in_month,
        count(distinct si.user_id) as num_signed_in_users

    from sign_ins as si
    left join users u 
        on si.user_id = u.user_id
    left join school_years sy
        on sign_in_at 
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(5) }}
)

select * 
from final
