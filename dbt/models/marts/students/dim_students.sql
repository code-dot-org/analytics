with 
students as (
    select *    
    from {{ ref('dim_users')}}
    where user_type = 'student'
),

school_years as (
    select * from {{ref('int_school_years')}}
),

final as (
    select 
        students.*, 
        sy.school_year                                          as created_at_school_year
    from students 
    left join school_years                                      as sy 
        on students.created_at 
            between sy.started_at 
                and sy.ended_at )

select * 
from final 
