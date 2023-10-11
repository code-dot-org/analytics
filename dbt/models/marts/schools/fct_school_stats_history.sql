with 
school_stats_by_years as (
    select 
        school_id,
        school_year,
        total_students,
        count_student_am,
        count_student_as,
        count_student_hi,
        count_student_bl,
        count_student_wh,
        count_student_hp,
        count_student_tr,
        total_frl_eligible_students,

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
    {{ dbt_utils.group_by(11) }}
),

combined as (
    select 
        school_id,
        school_year,
        total_students,
        count_student_am,
        count_student_as,
        count_student_hi,
        count_student_bl,
        count_student_wh,
        count_student_hp,
        count_student_tr,
        total_frl_eligible_students,
        total_urg_no_tr_students,
        total_urg_students,
        total_students_calculated,

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
    school_id,
    school_year,
    total_students,
    count_student_am,
    count_student_as,
    count_student_hi,
    count_student_bl,
    count_student_wh,
    count_student_hp,
    count_student_tr,
    total_frl_eligible_students,
    total_urg_no_tr_students,
    total_urg_students,
    total_students_calculated,
    urg_percent,
    urg_no_tr_percent,
    -- cast(urg_percent_true as decimal(3,2)) as urg_percent_true,              -- (ag) check validity of this with business cases
    frl_eligible_percent
from combined 