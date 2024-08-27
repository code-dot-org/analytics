with
scripts as (
    select * 
    from {{ ref('base_dashboard__scripts') }}
),

renamed as (
    select
        script_id,
        script_name,
        wrapup_video_id,
        user_id,
        login_required,
        new_name,
        family_name,
        published_state,
        instruction_type,
        instructor_audience,
        participant_audience,

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
        end as course_name,
        
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
            script_name,
            '((-)+\\d{4})',
            '')                     as unit,

        created_at,
        updated_at
    from scripts )
    
select * 
from renamed
