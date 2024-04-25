with 
users as (
    select *
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
        extract(year from u.created_at)         as created_year,
        extract(month from u.created_at)        as created_month,
        to_char(u.created_at::date, 'YYYY-MM')  as created_month_year,
        count(distinct u.user_id)               as num_users_created

    from users          as u
    join school_years   as sy
        on u.created_at
            between sy.started_at 
                and sy.ended_at

    {{ dbt_utils.group_by(7) }} )

select *
from final
