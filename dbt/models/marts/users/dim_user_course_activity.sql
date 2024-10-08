/* 
Updated 1/31/24 - NZM: Given that all users' activity is included, made changes for consistency:
        Unrestricted courses
        Added user_type by joining to stg_dashboard__users
        Filtered user_levels to bring in only those with non-zero attempts

Updated 1/24/24: Filtered out certain courses from "counting" toward a student's being active

Updated 12/6/23:

1. Design:
    student_id int,
    school_year varchar(10),
    course_name varchar(3),
    last_activity_at timestamp

2. Definitions:
    - a user is included in this table if they attempted a level of a given course within a given SY
    - this table is used to determine "active" students within a section and assigning a "section_status"

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
        user_id,
        level_id,
        script_id,
        created_at
    from {{ ref('stg_dashboard__user_levels') }}
    where attempts > 0
),

course_structure as (
    select  
        course_name, 
        level_id, 
        script_id 
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
),

combined as (
    select 
        ul.user_id,
        u.user_type,
        sy.school_year,
        u.us_intl,
        u.country,
        cs.course_name,
        min(ul.created_at)                      as first_activity_at,
		max(ul.created_at)                      as last_activity_at

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
                
    {{ dbt_utils.group_by(6) }} )

select *
from combined