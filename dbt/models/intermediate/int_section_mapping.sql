/* 
Model: int_section_mapping
Design:
    - student_id
    - school_year
    - section_id
    - teacher_id
    - is_section_owner
    - school_id


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

section_instructors as (
    select *
    from {{ ref('stg_dashboard__section_instructors') }}
),

-- 2a. incorporate missing teachers 
teacher_sections as (
    select 
        section_id, 
        teacher_id 
    from sections
    union all 
    select 
        section_id, 
        instructor_id
    from section_instructors 
),

-- 2b. assign section owner
section_owners as (
    select 
        ts.section_id, 
        ts.teacher_id,
        
        case when ts.teacher_id = sec.teacher_id
        then 1 else 0 end as is_section_owner

    from teacher_sections   as ts
    left join sections      as sec 
        on ts.section_id = sec.section_id 
),

-- 2c. bring in school_id
teachers as (
    select 
        seco.section_id,
        seco.teacher_id,
        seco.is_section_owner,
        tsc.school_id,
        tsc.started_at_sy as school_year 

        tsc.school_info_id,
        row_number() over(
            partition by 
                followers.student_id, 
                sy.school_year
                order by 
                    followers.created_at
        ) as row_num
    from section_owners         as seco
    left join teacher_school_changes as tsc 
        on seco.teacher_id = tsc.teacher_id
),

-- 3. prep student data 
students as (
    select 
        student_id,
        section_id,
        school_year
    from (
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
                    and sy.ended_at ) 
    where row_num = 1
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
    
    from students       as stu 
    left join teachers  as tea 
        on stu.section_id = tea.section_id 
        and stu.school_year = tea.school_year )

select *
from combined 