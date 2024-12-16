with 

self_paced_activity as ( 
    select 
        teacher_id
        , level_created_school_year         as school_year
        , case 
            when course_name like '%csd%' then 'csd'
            when course_name like '%csf%' then 'csf'
            when course_name like '%csp%' then 'csp'
            when course_name like '%csa%' then 'csa'
            when course_name like '%csc%' then 'csc'
        end                                 as course_name
        , 'self_paced'                      as activity_type
        , content_area
        , min(level_created_dt)             as first_activity_at
        , max(level_created_dt)             as last_activity_at
        , count(distinct level_script_id)   as num_levels 
    from {{ ref('dim_self_paced_pd_activity') }}
    group by 1,2,3,4,5
),

teacher_schools_historical as (
    select * 
    from {{ ref('int_teacher_schools_historical') }}
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
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

regional_partners as (
    select * 
    from {{ ref('dim_regional_partners') }}
),

pl_grade_band_mappings as (
    select * 
    from {{ ref('stg_external_datasets__pl_grade_band_mappings') }}
),

workshop_activity as (
    select
        
)

select 
    spa.teacher_id
    , spa.school_year
    , spa.course_name               as topic_area
    , spa.activity_type
    , spa.first_activity_at
    , spa.last_activity_at
    , spa.num_levels
    , spa.content_area
    , tsh.school_id
    , schools.school_district_id

from self_paced_activity spa
left join teacher_schools_historical tsh
    on spa.teacher_id = tsh.teacher_id
    and spa.school_year = tsh.started_at_sy 
left join schools 
    on tsh.school_id = schools.school_id