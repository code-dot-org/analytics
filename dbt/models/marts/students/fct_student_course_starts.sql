/* 
1. Design:
    student_id int,
    school_year varchar(10),
    user_level_created_at timestamp,
    course_name varchar(3),
    script_id int,
    stage_id int,
    level_id int

2. Definitions:
    - a student is included in this table if they attempted a level of a given course within a given SY
    - this table is used to determine "active" students within a section and assigning a "section_status"

3. Sources:
    dim_user_levels
    course_structure
    school_years

Ref: dataops-316
*/

with user_levels as (
    select * 
    from {{ ref('dim_user_levels') }}
),

course_structure as (
    select * 
    from {{ ref('dim_course_structure') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
),

combined as (
    select ul.user_id as student_id
		,sy.school_year
		,ul.user_level_created_at
		,cs.course_name_true as course_name
		,cs.script_id
		,cs.stage_id
		,cs.level_id
        ,row_number() over (partition by user_id, sy.school_year, course_name_true order by ul.user_level_created_at) as row_num  -----------------script id ordering
	from user_levels ul 
	join course_structure cs
		on ul.script_id = cs.script_id and ul.level_id = cs.level_id 
	join school_years sy 
		on ul.user_level_created_at between sy.started_at and sy.ended_at --------------  In order to select one record per user per course per year we need to attach the activty to a school_year
	where ul.attempts > 0  -- 0 attempts are ul records we want to ignore

),

final as (
    select student_id
        ,school_year
        ,user_level_created_at
        ,course_name
        ,script_id
        ,stage_id
        ,level_id 
    from combined
    where row_num = 1
)

select * 
from final

