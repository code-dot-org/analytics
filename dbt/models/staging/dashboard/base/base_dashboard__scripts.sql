with 
source as (
    select * 
    from {{ source('dashboard', 'scripts') }}
),

renamed as (
    select
        id          as script_id,
        name        as script_name,
        created_at,
        updated_at,
        wrapup_video_id,
        user_id,
        login_required,
        lower(new_name)                 as new_name,
        lower(family_name)              as family_name,
        lower(published_state)          as published_state,
        lower(instruction_type)         as instruction_type,
        lower(instructor_audience)      as instructor_audience,
        lower(participant_audience)     as participant_audience,

        -- json extraction fields 
        case 
            when json_extract_path_text(
                properties, 
                'curriculum_umbrella') = ''                         then 'other'
            else lower(
                json_extract_path_text(
                    properties, 
                    'curriculum_umbrella',
                true))
        end                         as course_name,
        
        json_extract_path_text(
            properties, 
            'supported_locales')    as supported_locales,
        
        json_extract_path_text(
            properties,
            'version_year')         as version_year,
        
        json_extract_path_text(
            properties,
            'is_course')            as is_standalone,
        
        regexp_replace(
            name,
            '((-)+\\d{4})',
            '')                     as unit,

        json_extract_path_text(
            properties, 
            'content_area', true)   as content_area,

        json_extract_path_text(
                properties, 
                'topic_tags', true) as topic_tags
    from source )

select *
from renamed
