with 
 __dbt__cte__base_dashboard_pii__users as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."users"
    where not deleted_at
),

renamed as (
    select
        id                          as user_id,
        studio_person_id,
        sign_in_count,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        -- username,
        provider,
        uid,
        admin,
        -- gender,
        -- name,
        locale,
        -- birthday,
        user_type,
        school,
        full_address,
        school_info_id,
        total_lines,
        active                      as is_active,
        purged_at,
        invited_by_id,
        invited_by_type,
        terms_of_service_version,
        urm                         as is_urm,
        -- races,
        primary_contact_info_id

    from source
)

select * 
from renamed
), users as (
    select * 
    from __dbt__cte__base_dashboard_pii__users
)

select * 
from users