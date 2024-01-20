with 
 __dbt__cte__base_dashboard__sections as (
with 
source as (
    select * 
    from "dashboard"."dashboard_production"."sections"
    where deleted_at is null  
),

renamed as (
    select
        -- section data
        id                      as section_id,
        name                    as section_name,
        section_type,

        user_id,
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
        first_activity_at
    from source
)

select * 
from renamed
), sections as (
    select *
    from __dbt__cte__base_dashboard__sections
)

select * 
from sections