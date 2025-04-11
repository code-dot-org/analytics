/* 
Updated 1/31/24 - NZM: Given that all users' activity is included, made changes for consistency:
        Unrestricted courses
        Added user_type by joining to stg_dashboard__users
        Filtered user_levels to bring in only those with non-zero attempts

Updated 1/24/24: Filtered out certain courses from "counting" toward a student's being active

Updated 12/6/23:

Updated 10/28/24: Added activity stats for users by course and school year: num_levels, num_unique_days, sum_time_spent

1. Design:
    student_id int,
    school_year varchar(10),
    course_name varchar(3),
    last_activity_at timestamp,
    num_levels  int,
    num_unique_days int,
    sum_time_spent  int

2. Definitions:
    - a user is included in this table if they attempted a level of a given course within a given SY
    - this table is used to determine "active" students within a section and assigning a "section_status"
    - num_levels: number of distinct levels of a course attempted by the student in the school year
    - num_unique_days: number of distinct days in a school year when the student had activity 
    - sum_time_spent: sum of time spent across programming levels of the course in the school year

3. Sources:
    stg_dashboard__user_levels
    course_structure
    school_years
    stg_dashboard__users

Ref: dataops-316
*/

with 
user_levels as (
    select 
        user_level_id,
        user_id,
        level_id,
        script_id,
        created_at,
        time_spent
    from {{ ref('stg_dashboard__user_levels') }}
    where attempts > 0
),

course_structure as (
    select  
        content_area,
        course_name, 
        topic_tags,
        level_id, 
        script_id, 
        level_type
    from {{ ref('dim_course_structure') }}
    where is_active_student_course = 1
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

users as (
    select *
    from {{ ref('dim_users') }}
    where user_type = 'student'
),

combined as (
    select 
        ul.user_id,
        u.user_type,
        sy.school_year,
        u.us_intl,
        u.country,
        cs.content_area,
        cs.course_name,
        cs.topic_tags,
        min(ul.created_at)                      as first_activity_at,
		max(ul.created_at)                      as last_activity_at,
        count(distinct ul.user_level_id)        as num_levels,
        count (distinct 
                case 
                    when cs.level_type in (
                        'bubblechoice'
                        , 'curriculumreference'
                        , 'external'
                        , 'externallink'
                        , 'freeresponse'
                        , 'levelgroup'
                        , 'map'
                        , 'match'
                        , 'multi'
                        , 'panels'
                        , 'standalonevideo'
                        , 'unplugged'
                        )
                        then null 
                    else ul.user_level_id end )      as num_levels_course_progress, -- levels that count for course progress because they indicate on-platform activity. Used mostly for 6-12 curriculum
        count(distinct trunc(ul.created_at))    as num_unique_days,
        sum(time_spent)                         as sum_time_spent

	from user_levels ul 
    join users u
        on ul.user_id = u.user_id
	join course_structure cs
		on ul.script_id = cs.script_id 
        and ul.level_id = cs.level_id 
	join school_years sy 
		on ul.created_at 
            between sy.started_at 
                and sy.ended_at
                
    {{ dbt_utils.group_by(8) }} )

select *
from combined