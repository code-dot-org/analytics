with
pd_enrollments as (
    select * 
    from {{ ref('base_dashboard_pii__pd_enrollments') }}
)

select 
    pd_enrollment_id
    , pd_workshop_id
    , name
    , enrolled_at 
    , updated_at
    , user_entered_school
    , teacher_id
    , survey_sent_dt
    , completed_survey_id                                   
    , school_info_id
    , application_id 

from pd_enrollments
where enrolled_at > {{ get_cutoff_date() }} 