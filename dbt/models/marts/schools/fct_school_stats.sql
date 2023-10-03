with 
school_stats_by_years as (
    select *
    from {{ ref('stg_dashboard__school_stats_by_years') }}
),

combined as (
    select *,
        -- calculations 
        sum(count_student_am 
            + count_student_as
            + count_student_hi
            + count_student_bl
            + count_student_wh
            + count_student_hp) as total_urg_no_tr_students,

        sum(count_student_am
            + count_student_as
            + count_student_hi
            + count_student_bl
            + count_student_wh
            + count_student_hp
            + count_student_tr) as total_urg_students,

        sum(count_student_am  
            + count_student_as  
            + count_student_hi  
            + count_student_bl  
            + count_student_wh  
            + count_student_hp
            + count_student_tr) as total_students_calculated,
        
        case when total_students = total_students_calculated
             then sum(count_student_am 
                + count_student_hi  
                + count_student_bl  
                + count_student_hp) 
                / total_students::float 
        end as urg_percent,
        
        case when .7 <= sum(count_student_am  
                + count_student_as  
                + count_student_hi  
                + count_student_bl  
                + count_student_wh  
                + count_student_hp  
                + count_student_tr)::float
                / total_students::float
            then sum(count_student_am
                + count_student_hi
                + count_student_bl
                + count_student_hp)
                / 
                sum(count_student_am
                + count_student_as
                + count_student_hi
                + count_student_bl
                + count_student_wh
                + count_student_hp
                + count_student_tr)::float 
        end as urg_percent_true,
        
        case when 0 < sum(count_student_am
                    + count_student_as
                    + count_student_hi
                    + count_student_bl
                    + count_student_wh
                    + count_student_hp)
            then sum(count_student_am 
                + count_student_hi
                + count_student_bl
                + count_student_hp)
                / 
                sum(count_student_am
                + count_student_as
                + count_student_hi
                + count_student_bl
                + count_student_wh  
                + count_student_hp)::float 
        end as urg_percent_no_tr,
        
        case 
            when total_frl_eligible_students is null 
            or total_students is null 
            or total_frl_eligible_students > total_students 
                then null
            else total_frl_eligible_students / total_students::float 
        end as frl_eligible_percent,    
        case 
            when total_frl_eligible_students is null 
            or total_students is null 
                then null 
            when (total_frl_eligible_students / total_students::float) > 0.5
            then 1 
            else 0 
        end as is_high_needs

    from school_stats_by_years
    {{ dbt_utils.group_by(41) }}
)

select * 
from combined 