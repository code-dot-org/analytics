with actives as (
    select *
    from {{ ref('dim_active_students') }}
)
select
    activity_date as "date",
    user_type_merged,
    country,
    us_intl,
    count(distinct(user_id)) num_actives
from actives
{{dbt_utils.group_by(4)}}
order by 1,2