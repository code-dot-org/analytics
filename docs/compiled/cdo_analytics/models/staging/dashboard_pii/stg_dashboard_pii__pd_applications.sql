with
 __dbt__cte__base_dashboard_pii__pd_applications as (
with 
source as (
      select * from "dashboard"."dashboard_production_pii"."pd_applications"
),

renamed as (
    select
        id  as pd_application_id,
        user_id,
        type,
        application_year,
        application_type,
        regional_partner_id,
        status,
        locked_at,
        notes,
        form_data,
        created_at,
        updated_at,
        course,
        response_scores,
        application_guid,
        accepted_at,
        properties,
        deleted_at,
        status_timestamp_change_log,
        applied_at
    from source
)

select * from renamed
), pd_applications as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_applications
)

select * 
from pd_applications