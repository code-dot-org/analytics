with 

ai_feedback as (
    select * 
    from {{ ref('stg_dashboard__ai_tutor_interaction_feedbacks') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select 
    ai_feedback.feedback_id 
    , ai_feedback.feature_name
    , ai_feedback.user_id
    , ai_feedback.thumbs_up 
    , ai_feedback.created_at
    , school_years.school_year 
    , ai_feedback.details
from ai_feedback
join school_years 
    on ai_feedback.created_at between school_years.started_at and school_years.ended_at