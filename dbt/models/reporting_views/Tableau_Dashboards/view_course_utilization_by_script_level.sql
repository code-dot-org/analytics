/* Created 12/09/24: Section actitity for each  Script, Lesson and Level for student-facing curriculum for the three most recent school years (current and two prior) for Course Utilization dashboard in Tableau
*/

with
school_years as (
    select * 
    from {{ref('int_school_years')}} 
    where sysdate-(365*1) < ended_at and sysdate > started_at -- three most recent school years (current and two prior), to change the number of years, change the multiplier
)


, student_activity as (
    select 
      sa.section_id
    , date_trunc('month',sa.activity_date) month_activity_date
    , sa.content_area
    , sa.course_name
    , sa.unit_name
    , sa.script_id
    , sa.level_id
    , sa.school_year
    , replace(sa.topic_tags,',survey','') as topic_tags
    , case when sa.topic_tags like '%survey%' then 1 else 0 end is_survey 
    , sa.us_intl
    , sa.country
    from {{ ref('dim_student_script_level_activity') }} sa
    join school_years sy on sa.school_year = sy.school_year -- limit to selected school years
    where 
    sa.user_type = 'student'
    and sa.content_area not in ('hoc', 'other')
    and sa.topic_tags is not null -- limiting to create a small extract to enable publishing
        {{ dbt_utils.group_by(12) }} -- grouping instead of select distinct to deduplicate records with better performance
)

, course_structure as ((
    select distinct
    cs.script_id
    , cs.script_name
    , cs.version_year
    , cs.topic_tags
    , cs.stage_name as lesson_name
    , cs.stage_number as lesson_number
    , cs.stage_number || ' - ' || cs.stage_name as lesson_number_name
    , cs.level_id
    , cs.level_name
    , cs.level_number
    , cs.level_number || ' - ' || cs.level_name as level_number_name
        from {{ ref('dim_course_structure')}} cs
    where 
        cs.content_area not in ('hoc', 'other')
))


select
sa.*
, cs.script_name
, cs.version_year
, cs.lesson_name
, cs.lesson_number 
, cs.lesson_number_name
, cs.level_name
, cs.level_number
, cs.level_number_name 
from student_activity sa
join course_structure cs 
        on sa.script_id = cs.script_id
        and sa.level_id = cs.level_id
    {{ dbt_utils.group_by(20) }} -- grouping instead of select distinct to deduplicate records with better performance