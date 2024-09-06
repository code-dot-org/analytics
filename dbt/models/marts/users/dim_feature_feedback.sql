with 

ai_tutor_feedback as (
    select 
        feature_name
        , user_id
        , satisfied
        , details
        , created_at 
    from {{ ref('stg_dashboard__ai_tutor_interaction_feedbacks') }}
),

new_feature_feedback as (
    select 
        feature_name
        , user_id
        , satisfied
        , details
        , created_at
    from {{ ref('stg_dashboard__new_feature_feedbacks') }}
),

lti_feedback as (
    select 
        feature_name
        , user_id
        , satisfied
        , details
        , created_at 
    from {{ ref('stg_dashboard__lti_feedbacks') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

all_feedback as (
    select * 
    from ai_tutor_feedback 
    union all
    select * 
    from new_feature_feedback 
    union all
    select * 
    from lti_feedback
),

final as (
    select 
        row_number() over (order by all_feedback.user_id) as feedback_id
        , all_feedback.feature_name
        , all_feedback.user_id
        , all_feedback.satisfied 
        , all_feedback.created_at
        , school_years.school_year 
        , all_feedback.details
    from all_feedback
    join school_years 
        on all_feedback.created_at 
        between school_years.started_at 
            and school_years.ended_at )
select * 
from final 
