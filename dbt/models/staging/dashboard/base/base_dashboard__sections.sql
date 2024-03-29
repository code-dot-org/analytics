with 
source as (
    select * 
    from {{ source('dashboard', 'sections') }}
),

renamed as (
    select
        -- section data
        id                      as section_id,
        name                    as section_name,
        section_type,
        user_id                 as teacher_id,
        login_type,
        code,
        script_id,
        course_id,
        grade,
        
        -- flags 
        stage_extras            as is_stage_extras,
        pairing_allowed         as is_pairing_allowed,
        sharing_disabled        as is_sharing_disabled,
        hidden                  as is_hidden,
        tts_autoplay_enabled    as is_tts_autoplay_enabled,
        restrict_section        as is_restrict_section,
        participant_type,
        -- properties,
        
        -- timestamps
        created_at,
        updated_at,
        first_activity_at,
        deleted_at
    from source
)
select * 
from renamed