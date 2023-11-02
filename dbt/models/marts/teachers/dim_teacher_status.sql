/*
dim_teacher_status

int_active_sections

users
teacher_id
school_year
teacher_status
num_sections 
courses_taught (comma separated list of unique courses associated with all sections taught that SY, only relevant for active teachers)
*/

with 
teachers as (
    select *
    from {{ ref('dim_teachers') }}
),

section_status as (
    select distinct
        teacher_id,
        section_id,
        school_year
    from {{ ref('int_section_mapping') }}
),

active_sections as (
    select distinct 
        teacher_id,
        section_id,
        school_year
    from {{ ref('int_active_sections') }}
),

combined as (
    select 
        teachers.teacher_id,
        
        -- active statuses: 
        case when 
    from teachers 
    left join section_status 
        on teachers.teacher_id = section_status.teacher_id 
)

{# 
Active retained: had an active section last SY and this SY

Active reacquired: had an active section before last SY, did not have one last SY, and have one this SY

Active new: the first SY the teacher has had an active section

Inactive churn: did not have an active section last SY or this SY

Inactive this year: had an active section last SY, but does not have one this SY

Market: has never had an active section
#}