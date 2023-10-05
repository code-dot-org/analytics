with 
school_stats_by_years as (
    select *,

        -- pre-process these totals here
        nullif(
            sum(count_student_am 
                + count_student_hi
                + count_student_bl
                + count_student_hp)
            ,0) as total_urg_no_tr_students,

        nullif(
            sum(count_student_am
                + count_student_hi
                + count_student_bl
                + count_student_hp
                + count_student_tr)
            ,0) as total_urg_students,

        nullif(
            sum(count_student_am  
                + count_student_as  
                + count_student_hi  
                + count_student_bl  
                + count_student_wh  
                + count_student_hp
                + count_student_tr)
            ,0) as total_students_calculated

    from {{ ref('stg_dashboard__school_stats_by_years') }}
    {{ dbt_utils.group_by(40) }}
),

combined as (
    select *,
        -- calculations 
        total_urg_students / total_students_calculated::float as urg_percent,

        total_urg_no_tr_students / total_students_calculated::float as urg_no_tr_percent,

        case when total_students_calculated / total_students >= .7
             then total_urg_students / total_students_calculated
        end as urg_percent_true,
        
        total_frl_eligible_students / total_students::float as frl_eligible_percent,  

        case when total_frl_eligible_students / total_students::float > 0.5
            then 1 else 0 
        end as is_high_needs

    from school_stats_by_years
)

select * 
from combined 