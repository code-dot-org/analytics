with
scripts as (
    select * 
    from {{ ref('base_dashboard__scripts') }}
),

renamed as (
    select
        script_id,
        lower(script_name)              as script_name,
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
    from scripts 
),

    final as (  
    select 
        *
        , case 
            when course_name in (
                'csc',
                'csf', 
                'csd', 
                'csa', 
                'csp', 
                'ai', 
                'foundations of cs'
            )
            then 1 
            else 0 
        end                                                             as is_active_student_course
    from renamed ) 

select * 
from final 