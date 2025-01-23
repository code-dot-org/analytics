with 
source as (
    select 
        id,
        key,
        display_name,
        created_at,
        updated_at,
        is_featured,
        assignable,
        curriculum_type,
        marketing_initiative,
        grade_levels,
        header,
        image,
        cs_topic,
        school_subject,
        device_compatibility,
        description,
        professional_learning_program,
        video,
        published_date,
        self_paced_pl_course_offering_id,
        ai_teaching_assistant_available
    from {{ source('dashboard', 'course_offerings') }}
)

select * 
from source