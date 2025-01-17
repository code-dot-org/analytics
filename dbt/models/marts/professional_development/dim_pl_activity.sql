with 

self_paced_activity as ( 
    select 
        teacher_id,
        level_created_school_year         as school_year,
        course_name_implementation        as topic,
        'self_paced'                      as pd_type,
        replace(replace(content_area, '_self_paced_pl', ''), 'self_paced_pl_', '') as grade_band,
        count(distinct level_script_id)   as num_levels 
    from {{ ref('dim_self_paced_pd_activity') }}
    {{ dbt_utils.group_by(5) }}
),

teacher_schools_historical as (
    select * 
    from {{ ref('int_teacher_schools_historical') }}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

districts as (
    select * 
    from {{ ref('dim_districts') }}
),

pd_attendances as (
    select 
        pd_attendance_id
        , pd_session_id
        , teacher_id
        , school_year
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
    where teacher_id is not null
),

pd_sessions as (
    select 
        pd_session_id
        , pd_workshop_id
        , school_year
        , num_hours 
    from {{ ref('stg_dashboard_pii__pd_sessions') }}
),

pd_workshops as (
    select 
        pd_workshop_id
        , organizer_id
        , school_year
        , course_name
        , subject as workshop_subject
        , started_at as workshop_started_at
        , regional_partner_id 
        , is_byow
    from {{ ref('stg_dashboard_pii__pd_workshops') }}
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
    select distinct
        course_name,
        content_area,
        replace(replace(replace(content_area, 'curriculum_', ''), '_self_paced_pl', ''), 'self_paced_pl_', '') as grade_band
    from {{ ref('dim_course_structure') }}
    where content_area != 'other'
),

regional_partners as (
    select * 
    from {{ ref('dim_regional_partners') }}
),

course_scripts as (
    select * 
    from {{ ref('stg_dashboard__course_scripts') }}
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

facilitated_pd as (
    select distinct 
        pda.teacher_id,
        pdw.school_year,
        'facilitated'                                   as pl_type,
        pdw.pd_workshop_id                              as pl_workshop_id,
        pdw.organizer_id                                as pl_organizer_id,
        pdw.regional_partner_id                         as workshop_regional_partner_id,
        districts.regional_partner_id                   as district_regional_partner_id,
        pdw.workshop_subject,
        pdw.workshop_started_at,
        pdw.is_byow,
        case 
            when pdw.course_name = 'build your own workshop' then co.display_name
            else pdw.course_name
        end                                             as topic,
        coalesce(cam.grade_band, cs.grade_band)         as grade_band,
        tsh.school_id,
        schools.school_district_id,
        cast(null as bigint)                            as num_levels,
        sum(pds.num_hours)                              as num_hours

from pd_attendances pda 
join pd_sessions pds
    on pda.pd_session_id = pds.pd_session_id
join pd_workshops pdw
    on pds.pd_workshop_id = pdw.pd_workshop_id
left join course_structure cs 
    on pdw.course_name = cs.course_name
left join course_offerings_pd_workshops copw 
    on pdw.pd_workshop_id = copw.pd_workshop_id
left join content_area_mapping cam 
    on copw.course_offering_id = cam.course_offering_id
left join course_offerings co 
    on copw.course_offering_id = co.course_offering_id
left join teacher_schools_historical tsh
    on pda.teacher_id = tsh.teacher_id
    and pda.school_year = tsh.started_at_sy
left join schools 
    on tsh.school_id = schools.school_id
left join districts
    on schools.school_district_id = districts.school_district_id
{{ dbt_utils.group_by(15) }}
),

self_paced_pd as (
    select distinct
        spa.teacher_id,
        spa.school_year,
        spa.pd_type                         as pl_type,
        cast(null as bigint)                as pl_workshop_id,
        cast(null as bigint)                as pl_organizer_id,
        cast(null as bigint)                as workshop_regional_partner_id,
        districts.regional_partner_id       as district_regional_partner_id,
        cast(null as bigint)                as is_byow,
        spa.topic,
        spa.grade_band,
        tsh.school_id,
        schools.school_district_id,
        spa.num_levels,
        cast(null as bigint)                as num_hours

    from self_paced_activity spa
    left join teacher_schools_historical tsh
        on spa.teacher_id = tsh.teacher_id
        and spa.school_year = tsh.started_at_sy 
    left join schools 
        on tsh.school_id = schools.school_id
    left join districts
        on schools.school_district_id = districts.school_district_id
),

combined as (
    select *
    from facilitated_pd

    union 

    select * 
    from self_paced_pd
)

select * 
from combined
