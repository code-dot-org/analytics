with 

teachers as (
    select * 
    from {{ ref('dim_teachers') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

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
    where school_id is not null
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
    select *
    from {{ ref('int_pl_workshops') }}
),

facilitated_pd as (
    select distinct 
        pda.teacher_id,
        teachers.us_intl,
        pdw.school_year,
        'facilitated'                                   as pl_type,
        pdw.pl_workshop_id,
        pdw.pl_organizer_id,
        pdw.pl_regional_partner_id                      as workshop_regional_partner_id,
        districts.regional_partner_id                   as district_regional_partner_id,
        pdw.workshop_subject,
        pdw.workshop_started_at,
        pdw.is_byow,
        pdw.topic,
        pdw.grade_band,
        tsh.school_id,
        schools.school_district_id,
        cast(null as bigint)                            as num_levels,
        sum(pds.num_hours)                              as num_hours

from pd_attendances pda 
join pd_sessions pds
    on pda.pd_session_id = pds.pd_session_id
join pd_workshops pdw
    on pds.pd_workshop_id = pdw.pl_workshop_id
left join school_years sy
    on pdw.school_year = sy.school_year
left join teacher_schools_historical tsh
    on pda.teacher_id = tsh.teacher_id
    and sy.ended_at between tsh.started_at and tsh.ended_at
left join teachers 
    on pda.teacher_id = teachers.teacher_id
left join schools 
    on tsh.school_id = schools.school_id
left join districts
    on schools.school_district_id = districts.school_district_id
{{ dbt_utils.group_by(16) }}
),

self_paced_pd as (
    select distinct
        spa.teacher_id,
        teachers.us_intl,
        spa.school_year,
        spa.pd_type                         as pl_type,
        cast(null as bigint)                as pl_workshop_id,
        cast(null as bigint)                as pl_organizer_id,
        cast(null as bigint)                as workshop_regional_partner_id,
        districts.regional_partner_id       as district_regional_partner_id,
        null                                as workshop_subject,
        cast(null as timestamp)             as workshop_started_at,
        cast(null as bigint)                as is_byow,
        spa.topic,
        spa.grade_band,
        tsh.school_id,
        schools.school_district_id,
        spa.num_levels,
        cast(null as bigint)                as num_hours

    from self_paced_activity spa
    left join school_years sy
        on spa.school_year = sy.school_year
    left join teacher_schools_historical tsh
        on spa.teacher_id = tsh.teacher_id
        and sy.started_at between tsh.started_at and tsh.ended_at
    left join teachers 
        on spa.teacher_id = teachers.teacher_id
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
