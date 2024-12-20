/* Created 12/09/24: View with activity data for student-facing curriculum for the two most recent school years (current and two prior) for Course Utilization dashboard in Tableau  
*/

with

school_years as (
    select * 
    from {{ref('int_school_years')}} 
    where sysdate-(365*1) < ended_at and sysdate > started_at -- two most recent school years (current and prior), to change the number of years, change the multiplier
)


, student_activity as (
    select sa.*
    , sy.started_at as sy_start_at
    from {{ ref('dim_student_script_level_activity') }} sa
    join school_years sy on sa.school_year = sy.school_year -- limit to selected school years
    where 
    user_type = 'student'
    and content_area not in ('hoc')
    -- and sa.topic_tags is not null -- limiting to create a small extract to enable publishing
)


, schools as 
(
    select school_id
    ,  case	
        when 	coalesce(dss.is_title_i,0) = 1 
		    or	coalesce(dss.frl_eligible_percent,0) > 0.4 
		    or	coalesce(dss.urg_no_tr_numerator_percent,0) > 0.3 
		    or	coalesce(dss.is_rural,0) = 1
        then 1 else 0 end school_is_uu
    from {{ref('dim_schools')}} dss
)



select
  sa.student_id
, case 
    when date_trunc('week',sa.activity_date) < sa.sy_start_at then sa.sy_start_at -- if the start of the week falls before the start of the school year, then the start of the school year 
    else date_trunc('week',sa.activity_date) 
    end week_activity_date
, sa.content_area
, sa.course_name
, sa.unit_name
, sa.section_id
, sa.section_teacher_id as teacher_id
, sa.school_id
, sa.school_year
, replace(sa.topic_tags,',survey','') as topic_tags
, case when sa.topic_tags like '%survey%' then 1 else 0 end is_survey 
, sa.us_intl
, sa.country
, coalesce(ss.school_is_uu,0) school_is_uu
, sa.school_state

from student_activity sa
left join schools ss on sa.school_id = ss.school_id
    {{ dbt_utils.group_by(15) }} -- grouping instead of select distinct to deduplicate records with better performance
