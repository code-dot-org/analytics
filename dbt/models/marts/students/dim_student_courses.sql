/* 
Updated 12/6/23:

1. Design:
    student_id int,
    school_year varchar(10),
    course_name varchar(3),
    last_activity_at timestamp

2. Definitions:
    - a student is included in this table if they attempted a level of a given course within a given SY
    - this table is used to determine "active" students within a section and assigning a "section_status"

3. Sources:
    dim_user_levels
    course_structure
    school_years

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
),

course_structure as (
    select  
        course_name_true, 
        level_id, 
        script_id 
    from {{ ref('dim_course_structure') }}
),

school_years as (
    select *
    from {{ ref('int_school_years') }}
),

combined as (
    select 
         ul.user_id             as student_id
		,sy.school_year
		,cs.course_name_true    as course_name
		,max(ul.created_at)     as last_activity_at
	from user_levels ul 
	join course_structure cs
		on ul.script_id = cs.script_id 
        and ul.level_id = cs.level_id 
	join school_years sy 
		on ul.created_at 
            between sy.started_at and sy.ended_at
    {{ dbt_utils.group_by(3) }}
)

select *
from combined