/*
-- where teacher has active section, then:
    -- active retained: had an active section last SY and has an active section this SY (could be the same section ID, but needs to have 5+ active students both SYs)
    -- active reacquired: did not have an active section last SY, but does have one this SY
    -- active new: never has had an active section before, but has one this SY
    -- inactive churn: did not have an active section last SY and does not have an active section this SY
    -- inactive this year: had an active section last SY, does not have one this SY
    -- market: has a teacher account but has never had an active section 
*/
with 
student_teacher_section_school as (
    select * 
    from {{ ref('int_student_teacher_section_school') }}
),

section_status as (
    select * 
    from {{ ref('int_section_status') }}
),

combined as (
    select 
        stss.school_year,
        stss.school_id,
        stss.teacher_id,
        
        ss.section_id,
        ss.is_active,
        row_number() over(partition by school_year, teacher_id order school_year desc) as row_num 
    from student_teacher_section_school as stss 
    left join section_status as ss 
        on stss.section_id = ss.section_id
),

final as (
    select 
        teacher_id, 
        school_id, 
        case when is_active 
)