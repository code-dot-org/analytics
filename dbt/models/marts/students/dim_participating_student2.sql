with

dssla as (
    select * 
    from {{ref('dim_student_script_level_activity')}}
    where 
        user_type = 'student' and
        country = 'united states' and 
        --school_year = '2023-24' and
        course_name in ('csa','csp','foundations of cs',
            'csf','csc','csd','ai','9-12 special topics')

)

select
    school_year,
    count(distinct student_id)
from dssla
group by school_year
order by school_year desc