with dssla as (
    select
        student_id,
        school_id,
        school_year,
        school_district_id,
        school_state,
        section_teacher_id,
        date_trunc('month', activity_date) as activity_month
    from 
        {{ref("dim_student_script_level_activity")}}
    where user_type = 'student'
    and country = 'united states'
    and course_name <> 'hoc'
    and school_id is not null
)

, first_active_month_student as (
    select 
        student_id, 
        school_id,
        school_year,
        min(activity_month) as first_activity_month 
    from dssla
    group by 1,2,3
)

, first_active_month_teacher as (
    select 
        section_teacher_id, 
        school_id,
        school_year,
        min(activity_month) as first_activity_month 
    from dssla
    group by 1,2,3
)

, combined_student as (
    select 
        dssla.student_id,
        dssla.school_id,
        dssla.school_year,
        dssla.activity_month,
        fam.first_activity_month as first_activity_month
    from dssla  
    left join first_active_month_student as fams
         on dssla.student_id = fams.student_id 
        and dssla.school_id = fams.school_id 
        and dssla.school_year = fams.school_year
)

, combined_teacher as (
    select 
        dssla.section_teacher_id,
        dssla.school_id,
        dssla.school_year,
        dssla.activity_month,
        famt.first_activity_month as first_activity_month
    from dssla  
    left join first_active_month_teacher as famt 
         on dssla.student_id = famt.student_id 
        and dssla.school_id = famt.school_id 
        and dssla.school_year = famt.school_year
),

, final_student as (
    select 
        activity_month,
        school_year,
        school_state,
        count(distinct student_id) as num_active_students,
        count(distinct case 
            when activity_month = first_activity_month
            then student_id end) as num_new_students
    from combined as com
    group by 1,2,3,4
)