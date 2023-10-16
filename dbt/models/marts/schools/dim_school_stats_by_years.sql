-- Note: this data is built using NCES data recevied

with 
school_stats_by_years as (
    select 
        *,

        -- pre-process these totals here
        sum(count_student_am 
            + count_student_hi
            + count_student_bl
            + count_student_hp)
        as total_urg_no_tr_students,

        sum(count_student_am
            + count_student_hi
            + count_student_bl
            + count_student_hp
            + count_student_tr)
         as total_urg_students,

        sum(count_student_am  
            + count_student_as  
            + count_student_hi  
            + count_student_bl  
            + count_student_wh  
            + count_student_hp
            + count_student_tr)
        as total_students_calculated

    from {{ ref('stg_dashboard__school_stats_by_years') }}
    
    {{ dbt_utils.group_by(40) }}
),

combined as (
    select 
        *,

        -- calculations 
        total_urg_students / total_students_calculated::float as urg_percent,

        total_urg_no_tr_students / total_students_calculated::float as urg_no_tr_percent,

        -- case when total_students_calculated / total_students >= .7
        --      then total_urg_students / total_students_calculated
        -- end as urg_percent_true,
        
        total_frl_eligible_students / total_students::float as frl_eligible_percent

    from school_stats_by_years
)

select 
    *
from combined 