with 

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

course_offerings as ( 
    select * 
    from {{ ref('stg_dashboard__course_offerings') }}
),

course_offerings_pd_workshops as ( 
    select * 
    from {{ ref('stg_dashboard_pii__course_offerings_pd_workshops') }}
),

course_structure as (
    select *
        , replace(replace(replace(content_area, 'curriculum_', ''), 'self_paced_pl_', ''), '_self_paced_pl', '') as grade_band
    from {{ ref('dim_course_structure') }}
    where content_area != 'other'
),

course_grade_band as (
    select distinct
        course_name,
        grade_band
    from course_structure
),

regional_partners as (
    select * 
    from {{ ref('dim_regional_partners') }}
),

content_area_mapping as (
    select distinct 
        wco.course_offering_id,
        cs.grade_band
    from course_offerings_pd_workshops wco 
    join course_offerings co 
        on wco.course_offering_id = co.course_offering_id 
    left join course_structure cs on co.key = cs.family_name
),

enrollments_by_workshop as ( 
    select 
        pd_workshop_id,
        coalesce(count(distinct teacher_id), 0) as num_teachers_enrolled 
    from pd_enrollments
    group by 1
),

sessions_by_workshop as (
    select 
        pd_workshop_id,
        coalesce(count(distinct pd_session_id), 0) as num_sessions,
        min(started_at):: date as workshop_started_at,
        max(started_at):: date as workshop_ended_at
    from pd_sessions
    group by 1
),

pd_workshops as (
    select 
        pdw.pd_workshop_id
        , pdw.organizer_id
        , pdw.school_year
        , pdw.course_name
        , pdw.subject as workshop_subject
        , pdw.workshop_name as workshop_name
        , pdw.regional_partner_id 
        , pdw.is_byow
        , pdw.participant_group_type
        , pdw.is_virtual
        , case 
            when sbw.workshop_started_at is null 
                or sbw.workshop_started_at > current_date
            then 1 
            else 0 
        end as is_upcoming
    from {{ ref('stg_dashboard_pii__pd_workshops') }} pdw
    left join sessions_by_workshop sbw 
        on pdw.pd_workshop_id = sbw.pd_workshop_id
),

session_teacher_attendance as (
    select 
        pda.teacher_id,
        pds.pd_workshop_id,
        coalesce(count(distinct pda.pd_session_id), 0) as num_sessions_attended
    from pd_attendances pda 
    left join pd_sessions pds 
        on pda.pd_session_id = pds.pd_session_id
    group by 1,2
),

attendances_by_workshop as ( 
    select 
        pd_workshop_id,
        coalesce(count(distinct teacher_id), 0) as num_teachers_attended 
    from session_teacher_attendance
    group by 1
),

avg_sessions_attended as (
    select 
        pd_workshop_id,
        avg(cast(num_sessions_attended as float)) as avg_sessions_attended
    from session_teacher_attendance
    group by 1
)

select 
    pd_workshops.pd_workshop_id as pl_workshop_id,
    pd_workshops.organizer_id as pl_organizer_id,
    pd_workshops.regional_partner_id as pl_regional_partner_id,
    pd_workshops.school_year,
    pd_workshops.workshop_subject,
    pd_workshops.workshop_name,
    pd_workshops.participant_group_type,
    pd_workshops.is_byow,
    pd_workshops.is_virtual,
    pd_workshops.is_upcoming,

    s.workshop_started_at,
    s.workshop_ended_at,

    case 
        when pd_workshops.course_name = 'build your own workshop' then co.display_name
        else pd_workshops.course_name
    end                                             as topic,
    coalesce(
        cam.grade_band, 
        cg.grade_band
    )                                               as grade_band,
    cast(
        e.num_teachers_enrolled as float
    )                                               as num_teachers_enrolled,
    case 
        when pd_workshops.is_upcoming = 1 then null 
        else 
            cast(
                a.num_teachers_attended as float
            )                                       
    end                                             as num_teachers_attended,
    case
        when pd_workshops.is_upcoming = 1 then null 
        else
            (
                cast(
                    a.num_teachers_attended as float
                ) / 
                nullif(
                    cast(
                    e.num_teachers_enrolled as float
                    ), 0
                )                                               
            )
    end                                             as pct_teachers_attended,
    cast(
        s.num_sessions as float
    )                                               as num_sessions,
    case
        when pd_workshops.is_upcoming = 1 then null 
        else
            round(
                asa.avg_sessions_attended, 3
            ) :: decimal(10,4)
    end                                             as avg_sessions_attended,
    case
        when pd_workshops.is_upcoming = 1 then null 
        else
            round(
                asa.avg_sessions_attended 
                / nullif(s.num_sessions, 0), 3
            ) :: decimal(10,4) 
    end                                             as pct_sessions_attended

from pd_workshops
left join enrollments_by_workshop e 
    on pd_workshops.pd_workshop_id = e.pd_workshop_id
left join attendances_by_workshop a 
    on pd_workshops.pd_workshop_id = a.pd_workshop_id
left join sessions_by_workshop s 
    on pd_workshops.pd_workshop_id = s.pd_workshop_id
left join avg_sessions_attended asa 
    on pd_workshops.pd_workshop_id = asa.pd_workshop_id
left join course_grade_band cg 
    on pd_workshops.course_name = cg.course_name
left join course_offerings_pd_workshops copw 
    on pd_workshops.pd_workshop_id = copw.pd_workshop_id
left join content_area_mapping cam 
    on copw.course_offering_id = cam.course_offering_id
left join course_offerings co 
    on copw.course_offering_id = co.course_offering_id

    

