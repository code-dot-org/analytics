with 

self_paced_activity as ( 
    select 
        teacher_id,
        level_created_school_year         as school_year,
        course_name_implementation        as topic,
        'self_paced'                      as pd_type,
        case 
            when content_area = 'self_paced_pl_k_5' then 'k_5'
            when content_area = 'self_paced_pl_6_8' then '6_8'
            when content_area = 'self_paced_pl_9_12' then '9_12'
            when content_area = 'skills_focused_self_paced_pl' then 'skills_focused'
            else null 
        end                               as grade_band,
        -- , min(level_created_dt)             as first_activity_at
        -- , max(level_created_dt)             as last_activity_at
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
        , school_year
    from {{ ref('stg_dashboard_pii__pd_attendances') }}
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
        , case 
            when course_name = 'csf' then 'k_5'
            when course_name = 'csc' then 'k_5'
            when course_name = 'csd' then '6_8'
            when course_name = 'csp' then '9_12'
            when course_name = 'csa' then '9_12'
        else null
        end as grade_band
        , subject
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

regional_partners as (
    select * 
    from {{ ref('dim_regional_partners') }}
),

pl_grade_band_mappings as (
    select * 
    from {{ ref('stg_external_datasets__pl_grade_band_mappings') }}
),

facilitated_pd as (
    select distinct 
        pda.teacher_id,
        pdw.school_year,
        'facilitated'                                   as pd_type,
        pdw.pd_workshop_id,
        pdw.organizer_id,
        pdw.regional_partner_id                         as workshop_regional_partner_id,
        districts.regional_partner_id                   as district_regional_partner_id,
        pdw.is_byow,
        case 
            when pdw.course_name = 'build your own workshop' 
            then co.display_name
            else pdw.course_name
        end                                             as topic,
        coalesce(mappings.grade_band, pdw.grade_band)   as grade_band,
        tsh.school_id,
        schools.school_district_id,
        -- pdw.subject,
        -- co.display_name
        --co.cs_topic,
        cast(null as bigint)                            as num_levels,
        sum(pds.num_hours)                              as num_hours

from pd_attendances pda 
join pd_sessions pds
    on pda.pd_session_id = pds.pd_session_id
join pd_workshops pdw
    on pds.pd_workshop_id = pdw.pd_workshop_id
left join course_offerings_pd_workshops copw 
    on pdw.pd_workshop_id = copw.pd_workshop_id
left join course_offerings co 
    on copw.course_offering_id = co.course_offering_id
left join pl_grade_band_mappings mappings 
    on co.display_name = mappings.topic
left join teacher_schools_historical tsh
    on pda.teacher_id = tsh.teacher_id
    and pda.school_year = tsh.started_at_sy
left join schools 
    on tsh.school_id = schools.school_id
left join districts
    on schools.school_district_id = districts.school_district_id
{{ dbt_utils.group_by(13) }}
),

self_paced_pd as (
    select distinct
        spa.teacher_id,
        spa.school_year,
        spa.pd_type,
        cast(null as bigint)                as pd_workshop_id,
        cast(null as bigint)                as organizer_id,
        cast(null as bigint)                as workshop_regional_partner_id,
        districts.regional_partner_id       as district_regional_partner_id,
        cast(null as bigint)                as is_byow,
        spa.topic,
        -- , spa.first_activity_at
        -- , spa.last_activity_at
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
