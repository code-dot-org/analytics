with actives as (
    select *
    from {{ ref('dim_active_students') }}
)
select
    date_trunc(month, activity_date) as "date",
    country,
    us_intl,
    count(distinct(user_id)) num_actives
from actives
{{dbt_utils.group_by(3)}}