/*
    Number of active students per year
*/
with actives as (
    select *
    from {{ ref('dim_active_students') }}
)
select
    school_year,
    user_type_merged,
    country,
    us_intl,
    count(distinct(user_id)) num_actives
from actives
{{dbt_utils.group_by(4)}}
order by 1,2