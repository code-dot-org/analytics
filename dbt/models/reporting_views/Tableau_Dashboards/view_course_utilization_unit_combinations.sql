/* Created 12/11/24: View with activity data for student-facing curriculum for the two most recent school years (current and prior) for Course Utilization dashboard in Tableau  
*/

with
school_years as (
    select * 
    from {{ref('int_school_years')}} 
    where sysdate-(365*1) < ended_at and sysdate > started_at -- two most recent school years (current and prior), to change the number of years, change the multiplier
)


, student_activity as (
    select sa.*
    from {{ ref('dim_student_script_level_activity') }} sa
    join school_years sy on sa.school_year = sy.school_year -- limit to selected school years
    where 
    user_type = 'student'
    and content_area not in ('hoc')
    -- and sa.topic_tags is not null -- limiting to create a small extract to enable publishing
)



select
  sa.section_id
, sa.school_year
, sa.content_area
, sa.course_name
, sa.us_intl
, sa.country
, listagg(distinct sa.unit_name,', ') within group (order by unit_name) unit_combination 
from student_activity sa
where 
 sa.topic_tags not like '%survey%'  
{{ dbt_utils.group_by(6) }} 