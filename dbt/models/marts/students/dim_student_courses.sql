/* 
Updated 1/24/24: Filtered out certain courses from "counting" toward a student's being active

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
		--,cs.course_name_true    as course_name

        -- Case-when below limits courses that count for students to the ones explicitly listed.
        --1. this should be replaced/fixed with a flag in course_structure that we can use to identify a true student course
        --2. I've explicitly showing my work by listing all course_name_true values - as of 1.24.24 - and commented out ones we're not including
       ,case 
        when cs.course_name_true in (  
            'ai',
            'csf',
            'csp',
            'csd',
            'hoc',
            'csc',
            'csa'
            -- 'other',
            -- 'csa virtual pl',
            -- 'csp virtual pl',
            -- 'csd virtual pl',
            -- 'csp self paced pl',
            -- 'csc self paced pl',
            -- 'csa self paced pl',
            -- 'csf self paced pl',
            -- 'csd self paced pl'
        ) then cs.course_name_true
        --else null
        end                     as course_name   


        ,min(ul.created_at)     as first_activity_at 
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
