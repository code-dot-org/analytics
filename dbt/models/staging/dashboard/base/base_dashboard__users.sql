with 
source as (
      select * 
      from {{ source('dashboard', 'users') }}
),

renamed as (
    select
        id                          as user_id,
        user_type,
        studio_person_id,
        primary_contact_info_id
        school_info_id,
        sign_in_count,
        total_lines,
        locale,
        gender,
        birthday,
        races,

        urm                         as is_urg,
        active                      as is_active,
        
        current_sign_in_at,
        last_sign_in_at,
        created_at,
        updated_at,
        purged_at,
        deleted_at
    from source )

select * 
from renamed