with dssla as (
    select * from 
    {{ref("dim_student_script_level_activity")}}
    where user_type = 'student'
    and country = 'united states'
    and course_name <> 'hoc'
    and school_id is not null
)

, first_active_month as (
    select 
        student_id, 
        school_id,
        school_year,
        min(activity_month) as first_activity_month --fix this 
    from dssla
    group by 1,2,3
)

, combined as (
    select 
        dssla.student_id,
        dssla.school_id,
        dssla.school_year,
        dssla.activity_month,
        fam.first_activity_month as first_activity_month
    from dssla  
    left join first_active_month as fam 
         on dssla.student_id = fam.student_id 
        and dssla.school_id = fam.school_id 
        and dssla.school_year = fam.school_year
)

select * from combined

monthly_aggregate as (
    select 
        school_year,
        activity_month,
        school_id,
        district_id,
        school_state,
        sum(distinct student_id) as num_students_month
        sum (distinct section_teacher_id) as num_teachers_month
    from 
        dssla
    group by 1,2,3,4,5
)

