with 
source as (
      select * 
      from {{ source('dashboard', 'users') }}
),

renamed as (
    select
        id                          as user_id,
        studio_person_id,
        primary_contact_info_id
        sign_in_count,
        locale,
        birthday,
        user_type,
        school_info_id,
        total_lines,
        active                      as is_active,
        urm                         as is_urg,
        gender,
        races,
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        purged_at,
        deleted_at
    from source
)

select * 
from renamed