with 
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

enrollments_with_course as (
    select 
        pde.*
        , pdw.course_name
        , pdw.school_year
        , pdw.regional_partner_id
    from pd_enrollments                                                     as pde 
    join pd_workshops                                                       as pdw
        on pde.pd_workshop_id = pdw.pd_workshop_id
    where lower(pdw.subject) in ('intro workshop', 'intro', 'deep dive', 'district')
    and pdw.course_name in ('csf')
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
    where lower(pdw.subject) in ('intro workshop', 'intro', 'deep dive', 'district')
    and pdw.course_name in ('csf')
    group by 1,2,3
)

select 
    pde.teacher_id
    , pde.regional_partner_id
    , pde.school_year
    , pde.course_name
    , case 
        when att.num_sessions_attended > 0 then 1 
        else 0
    end                                                                     as trained
from enrollments_with_course                                                as pde
left join attendances_by_workshop                                           as att
    on pde.teacher_id = att.teacher_id
    and pde.course_name = att.course_name
    and pde.school_year = att.school_year
where pde.course_name in ('csf')