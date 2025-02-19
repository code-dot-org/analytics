/* Created 01/01/28: View for students starting a course for Course Participation dashboard in Tableau  
This view has a record for every student/course/school year for student-facing curriculum, along with demographics and school characteristics 
*/

with

school_years as (
    select * 
    from {{ref('int_school_years')}} 
    where sysdate-(365*3) < ended_at and sysdate > started_at -- four most recent school years (current and prior 3), to change the number of years, change the multiplier
)

, student_demographics as (
    select 
        s.student_id
    ,   s.is_urg
    ,   s.gender_group
    ,   s.race_group 
    from {{ ref('dim_students') }} s
)


, student_activity as (
    select sa.*
    , sy.started_at as sy_start_at
    from {{ ref('dim_user_course_activity') }} sa
    join school_years sy on sa.school_year = sy.school_year -- limit to selected school years
    where 
    user_type = 'student'
    and content_area not in ('other')
)

, schools as 
(
    select 
        school_id
        , school_name
        , school_type
        , school_district_id
        , school_district_name
        , state                                         as school_state
        , coalesce(dss.is_title_i,0)                    as is_title_i 
		, coalesce(dss.frl_eligible_percent,0)          as frl_eligible_percent 
		, coalesce(dss.urg_no_tr_numerator_percent,0)   as urg_percent 
		, coalesce(dss.is_rural,0)                      as is_rural
        ,  case	
            when 	coalesce(dss.is_title_i,0) = 1 
		        or	coalesce(dss.frl_eligible_percent,0) > 0.4 
		        or	coalesce(dss.urg_no_tr_numerator_percent,0) > 0.3 
		        or	coalesce(dss.is_rural,0) = 1
            then 1 else 0 
            end school_is_uu
    from {{ref('dim_schools')}} dss
)

, section_mapping as (
    select *
    from {{ ref('int_section_mapping') }}
    
    where student_id in (
        select user_id 
        from student_activity )
                -- Restrict to students with relevant activity

)

, section_size as (
    select 
        section_id
        , count(distinct student_id) as section_size 
    from section_mapping
    {{ dbt_utils.group_by(1) }}
)

, student_section as (
    select 
        scm.*     
        , scz.section_size
    
    from section_mapping    as scm 
    join section_size       as scz 
        on scm.section_id   = scz.section_id
)

select 
  sa.user_id as student_id
, sa.school_year 
, sa.content_area
, sa.course_name
, sa.topic_tags
, sd.gender_group
, sd.race_group
, sd.is_urg
, sa.us_intl
, sa.country
, sa.num_unique_days
, sa.num_levels
, sa.num_levels_course_progress
, sec.school_id
, ss.school_name
, ss.school_type
, ss.is_title_i 
, ss.frl_eligible_percent 
, ss.urg_percent 
, ss.is_rural
, ss.school_is_uu
, ss.school_district_id
, ss.school_district_name
, ss.school_state
, sec.section_size

from student_activity   as  sa
left join student_demographics as sd
    on  sa.user_id      =   sd.student_id 
left join student_section      as  sec
    on  sa.user_id      =   sec.student_id
    and sa.school_year  =   sec.school_year
    and sa.first_activity_at    <=  sec.student_removed_at
    and sa.last_activity_at     >=  sec.student_added_at
left join schools       as ss
        on sec.school_id    = ss.school_id 

{{ dbt_utils.group_by(25) }} -- grouping instead of select distinct to deduplicate records with better performance
