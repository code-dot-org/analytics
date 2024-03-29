with 
users as (
    select
        user_id,
        created_at,
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
        sy.school_year                  as school_year,
        date_part(year, u.created_at)   as created_year,
        date_part(month, u.created_at)  as created_month,
        count(distinct u.user_id)       as num_users_created
    from users as u
    join school_years as sy
        on u.created_at
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(6) }}
)

select *
from final
