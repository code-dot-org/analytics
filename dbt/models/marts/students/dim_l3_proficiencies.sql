with 

user_proficiencies as (
    select * 
    from {{ ref('stg_dashboard__user_proficiencies') }}
),

students as ( 
    select * 
    from {{ ref('dim_students') }}
),

school_years as (
    select * 
    from {{ ref('int_school_years') }}
)

select distinct 
    user_id                                     as student_id, 
    extract(year from basic_proficiency_at)     as cal_year,
    school_years.school_year                    as school_year,
    students.country                            as country,
    basic_proficiency_at                        as l3_proficiency_at
from user_proficiencies
join students
    on user_proficiencies.user_id = students.student_id
join school_years 
    on user_proficiencies.basic_proficiency_at between school_years.started_at and school_years.ended_at
where basic_proficiency_at is not null