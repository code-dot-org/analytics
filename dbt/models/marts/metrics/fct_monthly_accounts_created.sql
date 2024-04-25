with 
users as (
    select
        user_id,
        user_type,
        us_intl,
        country,
        created_at, --timestamp
        date_trunc('day',created_at)::date      as created_dt,
        date_trunc('month',created_at)::date    as created_month,
        date_trunc('year',created_at)::date     as created_year,
        left(to_char(created_at, 'YYYY-MM'),7)  as created_year_month
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
        u.created_month,
        u.created_year,
        u.created_year_month,
        count(distinct u.user_id) as num_users_created

    from users          as u
    join school_years   as sy
        on u.created_dt
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(7) }}
)

select *
from final