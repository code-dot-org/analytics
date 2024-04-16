/* 
Model: int_section_mapping
Design:
    - school_year
    - student_id
    - teacher_id
    - section_id
    - is_section_owner

Ref: DATAOPS-321, 548
*/

-- 1. prep data
with 
school_years as (
    select *
    from {{ ref('int_school_years') }}
),

teacher_school_changes as (
    select *
    from {{ ref('int_teacher_schools_historical') }}
),

followers as (
    select *
    from {{ ref('stg_dashboard__followers') }}
),

sections as (
    select *
    from {{ ref('stg_dashboard__sections') }}
),

instructors as (
    select *
    from {{ ref('stg_dashboard__section_instructors') }}
),

-- 2a. incorporate missing teachers 
teacher_sections as (
    select 
        section_id, 
        instructor_id as teacher_id 
    from instructors 
    union -- all 
    select 
        section_id, 
        teacher_id 
    from sections
),

-- 2b. assign section owner
section_owners as (
    select 
        ts.section_id, 
        ts.teacher_id,
        
        case when ts.teacher_id = sec.teacher_id
        then 1 else 0 end as is_section_owner

    from teacher_sections   as ts
    left join sections     as sec 
        on ts.section_id = sec.section_id 
),

-- 2c. bring in school_id
teachers as (
    select 
        seco.*,
        tsc.school_id,
        tsc.started_at_sy as school_year 

    from section_owners         as seco
    join teacher_school_changes as tsc 
        on seco.teacher_id = tsc.teacher_id
),

-- 3. prep student data 
students as (
    select 
        foll.student_id,
        foll.section_id,
        sy.school_year,
        row_number() over(
            partition by 
                foll.student_id, 
                sy.school_year
            order by 
                sy.school_year) as row_num 
    from followers      as foll
    join school_years   as sy
        on foll.created_at
            between sy.started_at 
                and sy.ended_at
),

-- 4. combine teacher + student
combined as (
    select  
        stu.student_id,
        stu.school_year,
        tea.section_id,
        tea.teacher_id,
        tea.is_section_owner,
        tea.school_id
    
    from students as stu 

    left join teachers as tea 
        on stu.section_id = tea.section_id 
        -- and stu.school_year = tea.school_year
    
    where stu.row_num = 1 )

select * from combined 

/*************************************
## testing: 

-- gut check counts 
select school_year, 
    count(distinct section_id)  as num_sections, 
    count(distinct student_id)  as num_students,
    count(distinct teacher_id)  as num_teachers,
    count(distinct case 
        when is_section_owner = 0 
        then teacher_id end)    as num_coteachers,
    count(distinct school_id)   as num_schools
from combined 
group by 1
order by 1 desc 

-- test one id [section_id = 5181677]
select school_year, section_id, is_section_owner, count(distinct teacher_id)
from combined 
where section_id = 5181677
group by 1,2,3

vs 

select
    case when sei.instructor_id = sec.user_id then 1 else 0 end as is_section_owner,
    count(distinct instructor_id)
from dashboard.dashboard_production.section_instructors as sei
left join dashboard.dashboard_production.sections       as sec 
    on sei.section_id = sec.id
where section_id = 5181677
group by 1

****************************/