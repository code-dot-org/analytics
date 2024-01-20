with 
 __dbt__cte__base_dashboard_pii__user_school_infos as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."user_school_infos"
),

renamed as (
    select
        id                      as user_school_info_id,
        user_id,
        start_date              as started_at,
        end_date                as ended_at,
        school_info_id,
        last_confirmation_date  as last_confirmation_at,
        created_at,
        updated_at
    from source
)

select * 
from renamed
), user_school_infos as (
    select 
        user_school_info_id,
        user_id,
        started_at,
        case when ended_at is not null then ended_at else '9999-12-31' end as ended_at,
        school_info_id,
        last_confirmation_at,
        created_at,
        updated_at 
    from __dbt__cte__base_dashboard_pii__user_school_infos
)

select * 
from user_school_infos