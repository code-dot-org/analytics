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
        as total_students_calculated,

        sum(count_student_am  
            + count_student_as  
            + count_student_hi  
            + count_student_bl  
            + count_student_wh  
            + count_student_hp)
        as total_students_no_tr_calculated

    from {{ ref('stg_dashboard__school_stats_by_years') }}
    
    {{ dbt_utils.group_by(40) }}
),

combined as (
    select 
        *,

        -- calculations 
        total_urg_students / nullif(total_students_calculated,0)::float as urg_with_tr_percent,

        total_urg_no_tr_students / nullif(total_students_no_tr_calculated,0)::float as urg_no_tr_percent,

        total_urg_no_tr_students / nullif(total_students_calculated,0)::float as urg_percent,

        total_frl_eligible_students / nullif(total_students,0)::float as frl_eligible_percent

    from school_stats_by_years
)

select *
from combined 