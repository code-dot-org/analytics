with 

pd_applications as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_applications') }}
),

pd_enrollments as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_enrollments') }}
),

pd_attendances as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
),

pd_sessions as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_sessions') }}
),

pd_workshops as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_workshops') }}
),

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
),

int_application_status_times as (
    select * 
    from {{ ref('int_application_status_times') }}
),

regional_partners as (
    select * 
    from {{ ref('stg_dashboard_pii__regional_partners') }}
),

enrollments_with_workshop_info as (
    select pde.pd_enrollment_id
        , pde.application_id
        , pde.pd_workshop_id 
        , pde.teacher_id
        , pde.enrolled_at
        , pdw.school_year 
        , pdw.course_name
        , pdw.regional_partner_id
    from pd_enrollments                                                     as pde
    join pd_workshops                                                       as pdw
        on pde.pd_workshop_id = pdw.pd_workshop_id
),

all_in_funnel as (
    select distinct 
        teacher_id
        , course_name
        , school_year
    from pd_applications 
    union 
    select distinct 
        teacher_id
        , course_name
        , school_year 
    from enrollments_with_workshop_info
)

select distinct 
    af.teacher_id 
    , pda.pd_application_id
    , teachers.teacher_email
    , af.course_name
    , af.school_year
    , coalesce (pda.submitted, 0)                                       as submitted
    , coalesce(pda.submission_status, 'did not apply through Code.org') as submission_status
    , coalesce(pda.accepted,0)                                          as accepted
    , case
        when e.pd_enrollment_id is not null then 1 
        else 0
    end                                                                 as enrolled
    , case 
        when att.pd_attendance_id is not null then 1
        else 0
    end                                                                 as attended
    , pda.admin_approval_required
    , pda.admin_approval_received
    , pda.scholarship_frl
    , pda.scholarship_urg
    , pda.how_heard
    , pda.how_heard_code_website
    , pda.how_heard_email
    , pda.how_heard_rp_website
    , pda.how_heard_rp_email
    , pda.how_heard_rp_event_workshop
    , pda.how_heard_teacher
    , pda.how_heard_administrator
    , pda.how_heard_conference  
    , pda.how_heard_other
    , pda.applied_at
    , pda.accepted_at
    , e.enrolled_at
    , datediff(day, pda.applied_at, pda.accepted_at)                            as days_applied_accepted
    , datediff(day, pda.applied_at, e.enrolled_at)                              as days_applied_enrolled
    , datediff(day, pda.accepted_at, e.enrolled_at)                             as days_accepted_enrolled
    , pda.current_status
    , coalesce(e.regional_partner_id, pda.regional_partner_id)                  as regional_partner_id
    , st.days_in_pending
    , st.days_in_unreviewed
    , st.days_in_incomplete
    , st.days_in_pending_space_availability
    , st.days_in_awaiting_admin_approval
    , rp.urg_guardrail_pct
    , rp.frl_guardrail_pct

from all_in_funnel                                                              as af
join teachers 
    on af.teacher_id = teachers.teacher_id
left join pd_applications                                                       as pda
    on pda.teacher_id = af.teacher_id 
    and pda.course_name = af.course_name 
    and pda.school_year = af.school_year
left join int_application_status_times                                          as st
    on pda.pd_application_id = st.pd_application_id
left join enrollments_with_workshop_info                                        as e
    on e.teacher_id = af.teacher_id 
    and e.course_name = af.course_name 
    and e.school_year = af.school_year
left join pd_sessions                                                           as pds 
    on pds.pd_workshop_id = e.pd_workshop_id 
left join pd_attendances                                                        as att 
    on att.pd_session_id = pds.pd_session_id 
    and att.teacher_id = e.teacher_id
left join regional_partners                                                     as rp
    on rp.regional_partner_id = 
    coalesce(
        e.regional_partner_id
        , pda.regional_partner_id
    )










