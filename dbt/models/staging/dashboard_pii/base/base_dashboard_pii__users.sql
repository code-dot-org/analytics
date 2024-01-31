with 
source as (
    select * 
    from {{ source('dashboard_pii', 'users') }}
),

renamed as (
    select
        id                          as user_id,
        studio_person_id,
        email,
        sign_in_count,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        deleted_at,
        username,
        provider,
        uid,
        admin,
        gender,
        name,
        locale,
        birthday,
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
        urm                         as is_urg,
        -- races,
        primary_contact_info_id

    from source
)

select * 
from renamed