with
 __dbt__cte__base_dashboard_pii__pd_enrollments as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."pd_enrollments"
    where not deleted_at
),

renamed as (
    select
        id as pd_enrollment_id,
        pd_workshop_id,
        name,
        -- first_name,
        -- last_name,
        -- email,
        created_at,
        updated_at,
        school,
        code,
        user_id,
        survey_sent_at,
        completed_survey_id,
        school_info_id,
        -- properties,
        application_id
    from source
)

select * 
from renamed
), pd_enrollments as (
    select * 
    from __dbt__cte__base_dashboard_pii__pd_enrollments
)

select * 
from pd_enrollments