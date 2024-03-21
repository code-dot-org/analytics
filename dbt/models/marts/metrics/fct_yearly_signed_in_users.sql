{#
    model: fct_yearly_signed_in_users
    scope: same as month
    ref: dataops-593

    notes: 
    - we should make an update here to only run the metrics mart once a month on the last day
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
        us_intl,
        country
    from {{ ref('dim_users') }}
    where current_sign_in_at is not null -- exclude dummy accounts
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

final as (
    select
        u.user_type,                         
        u.us_intl,
        u.country,
        sy.school_year,
        extract(year from si.sign_in_at)    as sign_in_year,
        -- extract(month from si.sign_in_at)   as sign_in_month,
        count(distinct si.user_id)          as num_signed_in_users
    from sign_ins as si
    join users as u
        on si.user_id = u.user_id
    join school_years as sy
        on sign_in_at
            between sy.started_at 
                and sy.ended_at
    {{ dbt_utils.group_by(5) }})

select *
from final
