with 

base as (
    select * 
    from {{ ref('base_dashboard__course_offerings') }}
),

renamed as (
    select 
        id                                  as course_offering_id,
        -- key,
        lower(display_name)                 as display_name,
        -- created_at,
        -- updated_at,
        is_featured,
        assignable,
        lower(curriculum_type)              as curriculum_type,
        --marketing_initiative)
        grade_levels,
        
        -- header,
        -- image,
        lower(cs_topic)                     as cs_topic
        --lower(school_subject)           as school_subject,
        --device_compatibility,
        --description,
        --professional_learning_program,
        -- video,
        date_trunc('day', published_date)   as published_at,
        --self_paced_pl_course_offering_id,
        ai_teaching_assistant_available
    from base
)

select * 
from renamed