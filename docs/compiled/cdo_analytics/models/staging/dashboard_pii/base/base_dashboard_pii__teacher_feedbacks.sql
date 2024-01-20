with 
source as (
    select * 
    from "dashboard"."dashboard_production_pii"."teacher_feedbacks"
    where not deleted_at
),

renamed as (
    select
        id as teacher_feedback_id,
        -- comment,
        student_id,
        level_id,
        teacher_id,
        created_at,
        updated_at,
        performance,
        student_visit_count,
        student_first_visited_at,
        student_last_visited_at,
        seen_on_feedback_page_at,
        script_id,
        analytics_section_id,
        review_state
    from source
)

select * 
from renamed