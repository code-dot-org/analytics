{#
    model: 
    auth: cory
    notes:Uses the student data from ballmer_heavy_users and filters for schools with at least 5 students in the completed set for that school year and course
    changelog:
    #}

    with ballmer_heavy_users as (
        select * from {{ref('ballmer_heavy_user_students')}}
    ),

    schools as (
        select * from {{ref('dim_schools')}}
    ),

    student_counts as (
        select school_id,
            school_year,
            course_name,
            count (distinct user_id) as student_count,
            case when student_count >= 5 then 1 else 0 end heavy_user_school_flag
        from ballmer_heavy_users
        group by school_id, school_year, course_name
    ),

    final as (
        select 
        school_year,
        course_name,
        student_count,
        heavy_user_school_flag,
        student_counts.school_id
        from student_counts
        left join schools on student_counts.school_id = schools.school_id
    )

    select 
        *
    from final
    where heavy_user_school_flag = 1
