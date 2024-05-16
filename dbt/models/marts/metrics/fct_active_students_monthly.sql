/*
    Number of active students per month
*/
with actives as (
    select *
    from {{ ref('dim_active_students') }}
)
select
    date_trunc('month', activity_date)::date as "month_year",
    user_type_merged,
    country,
    us_intl,
    count(distinct(user_id)) num_actives
from actives
{{dbt_utils.group_by(4)}}
order by 1,2