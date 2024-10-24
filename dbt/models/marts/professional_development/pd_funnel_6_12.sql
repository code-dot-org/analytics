with 

pd_applications as (
    select * 
    from {{ ref('stg_dashboard_pii__pd_applications') }}
),

pd_enrollments as (
    select 
        pd_enrollment_id
        , pd_workshop_id
        , teacher_id
        , enrolled_at
    from {{ ref('stg_dashboard_pii__pd_enrollments') }}
    where teacher_id is not null
),

pd_attendances as (
    select 
        pd_attendance_id
        , pd_session_id
        , teacher_id
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
),

pd_sessions as (
    select 
        pd_session_id
        , pd_workshop_id
        , school_year
        , cal_year 
    from {{ ref('stg_dashboard_pii__pd_sessions') }}
),

pd_workshops as (
    select 
        pd_workshop_id
        , organizer_id
        , school_year
        , cal_year
        , course_name
        , subject
        , regional_partner_id 
    from {{ ref('stg_dashboard_pii__pd_workshops') }}
),

int_application_status_times as (
    select * 
    from {{ ref('int_application_status_times') }}
),

regional_partners as (
    select * 
    from {{ ref('stg_dashboard_pii__regional_partners') }}
),

enrollments_with_course as (
    select 
        pde.*
        , pdw.course_name
        , pdw.school_year
        , pdw.regional_partner_id
    from pd_enrollments                                                     as pde 
    join pd_workshops                                                       as pdw
        on pde.pd_workshop_id = pdw.pd_workshop_id
    where lower(pdw.subject) = '5-day summer'
    and pdw.course_name in ('csd', 'csp', 'csa')
),

attendances_by_workshop as (
    select 
        att.teacher_id
        , pdw.course_name
        , pdw.school_year
        , count(distinct att.pd_session_id) as num_sessions_attended
    from pd_attendances                                                     as att
    left join pd_sessions                                                   as pds
        on att.pd_session_id = pds.pd_session_id
    left join pd_workshops                                                  as pdw 
        on pds.pd_workshop_id = pdw.pd_workshop_id
    where lower(pdw.subject) = '5-day summer'
    and pdw.course_name in ('csd', 'csp', 'csa')
    group by 1,2,3
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
    from enrollments_with_course
)

select 
    af.teacher_id
    , coalesce (pde.regional_partner_id, pda.regional_partner_id)           as regional_partner_id 
    , af.school_year
    , af.course_name
    , case 
        when pda.teacher_id is not null then 1 
        else 0
    end                                                                     as applied
    , coalesce(pda.accepted,0)                                              as accepted
    , case 
        when pde.teacher_id is not null then 1 
        else 0
    end                                                                     as enrolled
    , case 
        when att.num_sessions_attended > 0 then 1 
        else 0
    end                                                                     as attended
    , coalesce(att.num_sessions_attended, 0)                                as num_sessions_attended
    , case
        when att.num_sessions_attended > 3 then 1
        else 0 
    end                                                                     as trained
    , pda.current_status
    , datediff(day, pda.applied_at, pda.accepted_at)                        as days_applied_accepted
    , datediff(day, pda.applied_at, pde.enrolled_at)                        as days_applied_enrolled
    , datediff(day, pda.accepted_at, pde.enrolled_at)                       as days_accepted_enrolled
    , st.days_in_pending
    , st.days_in_unreviewed
    , st.days_in_incomplete
    , st.days_in_pending_space_availability
    , st.days_in_awaiting_admin_approval
    , rp.urg_guardrail_pct
    , rp.frl_guardrail_pct
from all_in_funnel                                                          as af
left join pd_applications                                                   as pda 
    on af.teacher_id = pda.teacher_id 
    and af.course_name = pda.course_name
    and af.school_year = pda.school_year
left join int_application_status_times                                      as st
    on pda.pd_application_id = st.pd_application_id
left join enrollments_with_course                                           as pde
    on af.teacher_id = pde.teacher_id 
    and af.course_name = pde.course_name
    and af.school_year = pde.school_year
left join attendances_by_workshop                                           as att
    on af.teacher_id = att.teacher_id
    and af.course_name = att.course_name
    and af.school_year = att.school_year
left join regional_partners                                                 as rp
    on rp.regional_partner_id = 
    coalesce(
        pde.regional_partner_id
        , pda.regional_partner_id
    )
where af.course_name in ('csd', 'csp', 'csa')











